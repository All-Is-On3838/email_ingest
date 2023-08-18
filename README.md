# dropoff_email_ingest
Runs daily to upload dropoff data from outlook to bigquery.

## Cloud Function Configuration
Cloud Scheduler(run_dropoff_email_ingest) >>> Pub/Sub Topic (run_dropoff_email_ingest) >>> Pub/Sub Subscription(eventarc-us-central1-run-dropoff-ingest-385457-sub-285) >>> Cloud Function (run_dropoff_ingest)

### Cloud Function Also Needs...
- service account json file
- MSAL authority, client id, and app secret to be added as environment variables

## Local Test Code Uses...
- service account json file
- MSAL authority, client id, and app secret to be added to .env file
