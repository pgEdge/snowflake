# Release Procedure

* Verify that the new version's `snowflake--X.Y.sql` exists.
* Make sure that the `default_version` in `snowflake.control` is in sync with the new version.
* Verify that the upgrade script `snowflake--<old>-<new>.sql` exists.
* Thoroughly test everything, including the upgrade script.
* Create the tag with `git tag -a -m 'Version X.Y' vX.Y`
* Push the tag itself with `git push --tags`
