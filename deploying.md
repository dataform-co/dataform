TODO: Replace this with resurrected CI/CD docs.

# Open Source Continuous Deployment

Deploying Dataform means running Dataform pipelines according a schedule. This is [available via Dataform on GCP](https://cloud.google.com/dataform/docs), but basic scheduling can be achieved without. Here are two extremely lightweight ways to deploy Dataform projects on a recurring schedule.

## Absolute Baremetal

Configure this locally on a unix based machine.

Run `crontab -e` and add `0 9 * * * bash ~/run_dataform.sh > ~/$(date '+%Y-%m-%d_%H:%M:%S').txt` (or the desired cron), then save.

Copy the following script and place it at `~/run_dataform.sh`:

_Note: This set up assumes that you have checked in your .df-credentials.json file, which is insecure so should never be done._

```bash
#!/bin/bash
# TODO: Update all these fields.
REPOSITORY_NAME="<repository-name>"
REPOSITORY_URL="github.com/<username-or-organisation>/$REPOSITORY_NAME"
# NOTE: Storing personal access tokens in code is insecure, and should never be done.
PERSONAL_ACCESS_TOKEN="<personal-access-token>"

echo "Dataform run in progress $(date '+%Y-%m-%d_%H:%M:%S')"

cd /tmp

rm -rf $REPOSITORY_NAME

git clone https://user:$PERSONAL_ACCESS_TOKEN@$REPOSITORY_URL.git

cd $REPOSITORY_NAME

npm i

npm i -g @dataform/core

dataform run .
```
