/*-------------------------------------------------------------------------
 *
 * snowflake.c
 *    Snowflake style IDs for PostgreSQL
 *
 * Copyright (c) 2023, pgEdge, Inc.
 * Portions Copyright (c) 2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    snowflake/snowflake.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/bufmask.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_type.h"
#include "catalog/storage_xlog.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we could lose (skip over) as many values as we pre-logged.
 */
#define SEQ_LOG_VALS	32

/*
 * The "special area" of a sequence's buffer page looks like this.
 */
#define SEQ_MAGIC	  0x1717

typedef struct sequence_magic
{
	uint32		magic;
} sequence_magic;

/*
 * We store a SeqTable item for every sequence we have touched in the current
 * session.  This is needed to hold onto nextval/currval state.  (We can't
 * rely on the relcache, since it's only, well, a cache, and may decide to
 * discard entries.)
 */
typedef struct SeqTableData
{
	Oid			relid;			/* pg_class OID of this sequence (hash key) */
	Oid			filenumber;		/* last seen relfilenumber of this sequence */
	LocalTransactionId lxid;	/* xact in which we last did a seq op */
	bool		last_valid;		/* do we have a valid "last" value? */
	int64		last;			/* value last returned by nextval */
	int64		cached;			/* last value already cached for nextval */
	/* if last != cached, we have not used up all the cached values */
	int64		increment;		/* copy of sequence's increment field */
	/* note that increment is zero until we first do nextval_internal() */
} SeqTableData;

typedef SeqTableData *SeqTable;

typedef union Snowflake
{
	int64		sf_int64;
	struct
	{
		int64	sf_msec		:42;
		uint	sf_count	:12;
		uint	sf_node		:10;
	} sf;
} Snowflake;

#define SNOWFLAKE_EPOCH_OFFSET	1577836800L	/* 2020 - 1970 in seconds */

static HTAB *seqhashtab = NULL; /* hash table for SeqTable items */

/*
 * last_used_seq is updated by nextval() to point to the last used
 * sequence.
 */
static SeqTableData *last_used_seq = NULL;
static int32 snowflake_node_id = 0;

extern void _PG_init(void);

static Relation lock_and_open_sequence(SeqTable seq);
static void create_seq_hashtable(void);
static void init_sequence(Oid relid, SeqTable *p_elm, Relation *p_rel);
static Form_pg_sequence_data read_seq_tuple(Relation rel,
											Buffer *buf, HeapTuple seqdatatuple);

void
_PG_init(void)
{
    DefineCustomIntVariable("snowflake.node",
                            "Unique id of current node.",
                            NULL,
                            &snowflake_node_id,
                            0,
                            1,
                            1023,
                            PGC_SUSET,
                            0,
                            NULL,
                            NULL,
                            NULL);
}


/*
 * snowflake_nextval()
 *
 */
PG_FUNCTION_INFO_V1(snowflake_nextval);

Datum
snowflake_nextval(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	bool		check_permissions = true;

	SeqTable	elm;
	Relation	seqrel;
	Buffer		buf;
	Page		page;
	HeapTupleData seqdatatuple;
	Form_pg_sequence_data seq;
	int64		result;
	bool		logit = false;
	Snowflake	flake;
	struct timespec now;
	int64		now_msec;

	/* Check that GUC snowflake.node is set */
	if (snowflake_node_id == 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("value for snowflake.node is not set")));
		

	/* open and lock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (check_permissions &&
		pg_class_aclcheck(elm->relid, GetUserId(),
						  ACL_USAGE | ACL_UPDATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	/* read-only transactions may only modify temp sequences */
	if (!seqrel->rd_islocaltemp)
		PreventCommandIfReadOnly("nextval()");

	/*
	 * Forbid this during parallel operation because, to make it work, the
	 * cooperating backends would need to share the backend-local cached
	 * sequence information.  Currently, we don't support that.
	 */
	PreventCommandIfParallelMode("nextval()");

#if 0 /* TODO: we might want to bring back sequence caching later */
	if (elm->last != elm->cached)	/* some numbers were cached */
	{
		Assert(elm->last_valid);
		Assert(elm->increment != 0);
		elm->last += elm->increment;
		relation_close(seqrel, NoLock);
		last_used_seq = elm;
		PG_RETURN_INT64(elm->last);
	}
#endif

	/* lock page' buffer and read tuple */
	seq = read_seq_tuple(seqrel, &buf, &seqdatatuple);
	page = BufferGetPage(buf);

	/* with page locked we can get the current timestamp */
	clock_gettime(CLOCK_REALTIME, &now);
	now_msec = (now.tv_sec - SNOWFLAKE_EPOCH_OFFSET) * 1000 +
			   now.tv_nsec / 1000000;

	/* Check if the clock has advanced since last nextflake() call */
	flake.sf_int64 = seq->last_value;
	if (now_msec > flake.sf.sf_msec)
	{
		/* The clock has ticked, reset the counter */
		flake.sf.sf_msec = now_msec;
		flake.sf.sf_count = 0;
		logit = true;
	}
	else
	{
		/*
		 * The clock either has not ticked or is behind. We need to make
		 * sure that the flake doesn't move backwards and that we bump
		 * it into the future should the count roll over.
		 */
		flake.sf.sf_count++;
		if (flake.sf.sf_count == 0)
		{
			flake.sf.sf_msec++;
			logit = true;
		}
	}

	flake.sf.sf_node = snowflake_node_id;
	result = flake.sf_int64;

	/*
	 * Decide whether we should emit a WAL log record based on
	 * checkpoint.
	 */
	if (!logit)
	{
		XLogRecPtr	redoptr = GetRedoRecPtr();

		if (PageGetLSN(page) <= redoptr)
		{
			/* last update of seq was before checkpoint */
			logit = true;
		}
	}

	/* save info in local cache */
	elm->last = result;			/* last returned number */
	elm->cached = result;		/* last fetched number */
	elm->last_valid = true;

	last_used_seq = elm;

	/*
	 * If something needs to be WAL logged, acquire an xid, so this
	 * transaction's commit will trigger a WAL flush and wait for syncrep.
	 * It's sufficient to ensure the toplevel transaction has an xid, no need
	 * to assign xids subxacts, that'll already trigger an appropriate wait.
	 * (Have to do that here, so we're outside the critical section)
	 */
	if (logit && RelationNeedsWAL(seqrel))
		GetTopTransactionId();

	/* ready to change the on-disk (or really, in-buffer) tuple */
	START_CRIT_SECTION();

	/*
	 * We must mark the buffer dirty before doing XLogInsert(); see notes in
	 * SyncOneBuffer().  However, we don't apply the desired changes just yet.
	 * This looks like a violation of the buffer update protocol, but it is in
	 * fact safe because we hold exclusive lock on the buffer.  Any other
	 * process, including a checkpoint, that tries to examine the buffer
	 * contents will block until we release the lock, and then will see the
	 * final state that we install below.
	 */
	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (logit && RelationNeedsWAL(seqrel))
	{
		xl_seq_rec	xlrec;
		XLogRecPtr	recptr;
		Snowflake	log_flake;

		/*
		 * We don't log the current state of the tuple, but rather the state
		 * as it would appear after "log" more fetches.  This lets us skip
		 * that many future WAL records, at the cost that we lose those
		 * sequence values if we crash.
		 */
		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

		/*
		 * Set values that will be saved in xlog.
		 * We bump the millisecond in the last value one tick
		 * into the future of our current result. If a server
		 * can recover from a postmaster crash that fast and
		 * then come back into a workload of tens of thousands
		 * of sequence allocation per millisecond, we'd like
		 * to hear about it.
		 */
		log_flake.sf_int64 = flake.sf_int64;
		log_flake.sf.sf_msec++;
		seq->last_value = result;
		seq->is_called = true;
		seq->log_cnt = 0;

#if PG_VERSION_NUM >= 160000
		xlrec.locator = seqrel->rd_locator;
#else
		xlrec.node = seqrel->rd_node;
#endif

		XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
		XLogRegisterData((char *) seqdatatuple.t_data, seqdatatuple.t_len);

		recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

		PageSetLSN(page, recptr);
	}

	/* Now update sequence tuple to the intended final state */
	seq->last_value = result;		/* last fetched number */
	seq->is_called = true;
	seq->log_cnt = result;			/* how much is logged */

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);

	relation_close(seqrel, NoLock);

	PG_RETURN_INT64(result);
}

/*
 * snowflake_nextflake()
 *
 */
PG_FUNCTION_INFO_V1(snowflake_currval);

Datum
snowflake_currval(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		result;
	SeqTable	elm;
	Relation	seqrel;

	/* open and lock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (pg_class_aclcheck(elm->relid, GetUserId(),
						  ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	if (!elm->last_valid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("currval of sequence \"%s\" is not yet defined in this session",
						RelationGetRelationName(seqrel))));

	result = elm->last;

	relation_close(seqrel, NoLock);

	PG_RETURN_INT64(result);
}

/*
 * snowflake_get_epoch()
 *
 */
PG_FUNCTION_INFO_V1(snowflake_get_epoch);

Datum
snowflake_get_epoch(PG_FUNCTION_ARGS)
{
	Snowflake flake;

	flake.sf_int64 = PG_GETARG_INT64(0);

	PG_RETURN_NUMERIC(int64_div_fast_to_numeric((int64)flake.sf.sf_msec + SNOWFLAKE_EPOCH_OFFSET * 1000L, 3));
}

/*
 * snowflake_get_count()
 *
 */
PG_FUNCTION_INFO_V1(snowflake_get_count);

Datum
snowflake_get_count(PG_FUNCTION_ARGS)
{
	Snowflake flake;

	flake.sf_int64 = PG_GETARG_INT64(0);

	PG_RETURN_INT32((int32)flake.sf.sf_count);
}

/*
 * snowflake_get_node()
 *
 */
PG_FUNCTION_INFO_V1(snowflake_get_node);

Datum
snowflake_get_node(PG_FUNCTION_ARGS)
{
	Snowflake flake;

	flake.sf_int64 = PG_GETARG_INT64(0);

	PG_RETURN_INT64((int32)flake.sf.sf_node);
}

/*
 * Open the sequence and acquire lock if needed
 *
 * If we haven't touched the sequence already in this transaction,
 * we need to acquire a lock.  We arrange for the lock to
 * be owned by the top transaction, so that we don't need to do it
 * more than once per xact.
 */
static Relation
lock_and_open_sequence(SeqTable seq)
{
	LocalTransactionId thislxid = MyProc->lxid;

	/* Get the lock if not already held in this xact */
	if (seq->lxid != thislxid)
	{
		ResourceOwner currentOwner;

		currentOwner = CurrentResourceOwner;
		CurrentResourceOwner = TopTransactionResourceOwner;

		LockRelationOid(seq->relid, RowExclusiveLock);

		CurrentResourceOwner = currentOwner;

		/* Flag that we have a lock in the current xact */
		seq->lxid = thislxid;
	}

	/* We now know we have the lock, and can safely open the rel */
	return relation_open(seq->relid, NoLock);
}

/*
 * Creates the hash table for storing sequence data
 */
static void
create_seq_hashtable(void)
{
	HASHCTL		ctl;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(SeqTableData);

	seqhashtab = hash_create("Sequence values", 16, &ctl,
							 HASH_ELEM | HASH_BLOBS);
}

/*
 * Given a relation OID, open and lock the sequence.  p_elm and p_rel are
 * output parameters.
 */
static void
init_sequence(Oid relid, SeqTable *p_elm, Relation *p_rel)
{
	SeqTable	elm;
	Relation	seqrel;
	bool		found;

	/* Find or create a hash table entry for this sequence */
	if (seqhashtab == NULL)
		create_seq_hashtable();

	elm = (SeqTable) hash_search(seqhashtab, &relid, HASH_ENTER, &found);

	/*
	 * Initialize the new hash table entry if it did not exist already.
	 *
	 * NOTE: seqhashtab entries are stored for the life of a backend (unless
	 * explicitly discarded with DISCARD). If the sequence itself is deleted
	 * then the entry becomes wasted memory, but it's small enough that this
	 * should not matter.
	 */
	if (!found)
	{
		/* relid already filled in */
#if PG_VERSION_NUM >= 160000
		elm->filenumber = InvalidRelFileNumber;
#else
		elm->filenumber = InvalidOid;
#endif
		elm->lxid = InvalidLocalTransactionId;
		elm->last_valid = false;
		elm->last = elm->cached = 0;
	}

	/*
	 * Open the sequence relation.
	 */
	seqrel = lock_and_open_sequence(elm);

	if (seqrel->rd_rel->relkind != RELKIND_SEQUENCE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a sequence",
						RelationGetRelationName(seqrel))));

	/*
	 * If the sequence has been transactionally replaced since we last saw it,
	 * discard any cached-but-unissued values.  We do not touch the currval()
	 * state, however.
	 */
	if (seqrel->rd_rel->relfilenode != elm->filenumber)
	{
		elm->filenumber = seqrel->rd_rel->relfilenode;
		elm->cached = elm->last;
	}

	/* Return results */
	*p_elm = elm;
	*p_rel = seqrel;
}


/*
 * Given an opened sequence relation, lock the page buffer and find the tuple
 *
 * *buf receives the reference to the pinned-and-ex-locked buffer
 * *seqdatatuple receives the reference to the sequence tuple proper
 *		(this arg should point to a local variable of type HeapTupleData)
 *
 * Function's return value points to the data payload of the tuple
 */
static Form_pg_sequence_data
read_seq_tuple(Relation rel, Buffer *buf, HeapTuple seqdatatuple)
{
	Page		page;
	ItemId		lp;
	sequence_magic *sm;
	Form_pg_sequence_data seq;

	*buf = ReadBuffer(rel, 0);
	LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(*buf);
	sm = (sequence_magic *) PageGetSpecialPointer(page);

	if (sm->magic != SEQ_MAGIC)
		elog(ERROR, "bad magic number in sequence \"%s\": %08X",
			 RelationGetRelationName(rel), sm->magic);

	lp = PageGetItemId(page, FirstOffsetNumber);
	Assert(ItemIdIsNormal(lp));

	/* Note we currently only bother to set these two fields of *seqdatatuple */
	seqdatatuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
	seqdatatuple->t_len = ItemIdGetLength(lp);

	/*
	 * Previous releases of Postgres neglected to prevent SELECT FOR UPDATE on
	 * a sequence, which would leave a non-frozen XID in the sequence tuple's
	 * xmax, which eventually leads to clog access failures or worse. If we
	 * see this has happened, clean up after it.  We treat this like a hint
	 * bit update, ie, don't bother to WAL-log it, since we can certainly do
	 * this again if the update gets lost.
	 */
	Assert(!(seqdatatuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI));
	if (HeapTupleHeaderGetRawXmax(seqdatatuple->t_data) != InvalidTransactionId)
	{
		HeapTupleHeaderSetXmax(seqdatatuple->t_data, InvalidTransactionId);
		seqdatatuple->t_data->t_infomask &= ~HEAP_XMAX_COMMITTED;
		seqdatatuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
		MarkBufferDirtyHint(*buf, true);
	}

	seq = (Form_pg_sequence_data) GETSTRUCT(seqdatatuple);

	return seq;
}

