---
title: Use the REST API
subtitle: Learn how to use the REST API to control Dataform Web
priority: 8
---

<br/>
<div class="bp3-callout bp3-icon-info-sign bp3-intent-warning">
  The Dataform Web API is currently in Beta, and breaking changes are likely to happen.
</div>

## Authorization

Calls to the Dataform Web API are authenticated using API tokens. These can be created from the Dataform project's settings page.

When calling the API, you can pass your API token in an `Authorization` header, using a bearer token format.

For example, with `curl` these headers can be provided as follows:

```bash
curl -H "Authorization: Bearer 5235783003799552|s+N8gAs72qbi90pFEv7yW/KBImTshRdBoVKjjFA7lD0=|1" https://api.dataform.co/v1/project/1234/run/5678
```

## Creating runs

Runs can be created by making a `POST` call to the `RunCreate` method.

For detailed documentation on supported parameters, see the [`RunCreate`](api/reference#RunCreate) reference documentation.

For example, to create a run for the project ID `1234` and to trigger a specific schedule name:

```bash
curl -H "Authorization: Bearer 5235783003799552|s+N8gAs72qbi90pFEv7yW/KBImTshRdBoVKjjFA7lD0=|1" -X POST -d '{ "scheduleName": "some_schedule" }' https://api.dataform.co/v1/project/1234/run
```

This will return a [RunGetResponse](api/reference#/definitions/v1RunCreateResponse), that includes the created run's ID:

```json
{
  "id": "1029591293203"
}
```

## Getting run information

After creating a run, the status of the run can be checked with the [`RunGet`](api/reference#RunGet) method.

This should be a `GET` request to the appropriate path, for example for project ID `1234` and a run ID `5678`:

```bash
curl -H "Authorization: Bearer 5235783003799552|s+N8gAs72qbi90pFEv7yW/KBImTshRdBoVKjjFA7lD0=|1" https://api.dataform.co/v1/project/1234/run/5678
```

This will return a [`RunGetResponse`](api/reference#/definitions/v1RunGetResponse) such as:

```json
{
  "id": "5678",
  "status": "RUNNING",
  "runLogUrl": "https://app.dataform.co/#/1234/run/5678"
}
```

## Triggering a Dataform schedule from a 3rd party orchestration tool

Using the REST API it's possible to trigger Dataform schedules from a third party orchestration tool, like Airflow or Luigi.

The following example, written in Python, shows how you could initiate a schedule, check it's status every 10 seconds, and exit when the schedule either succeeds or fails.

```python
import requests
import time

base_url='https://api.dataform.co/v1/project/<PROJECT_ID>/run'
headers={'Authorization': 'Bearer <API_TOKEN>'}
run_create_request='{ "scheduleName": "<SCHEDULE_NAME>" }'

response = requests.post(base_url, data=run_create_request, headers=headers)

run_url = base_url + '/' + response.json()['id']

response = requests.get(run_url, headers=headers)
print(response.json())

while response.json()['status'] == 'RUNNING':
    time.sleep(10)
    response = requests.get(run_url, headers=headers)
    print(response.json())
```
