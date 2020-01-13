---
title: Using the API
priority: 1
---

Dataform Web can be controlled through a REST API. This is currently a paid feature, if you'd like to get set up please reach out to our team on Intercom on the bottom right of the page.

<div class="bp3-callout bp3-icon-info-sign bp3-intent-warning">
  The Dataform Web API is currently in Alpha, and breaking changes are likely to happen.
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

For detailed documentation on supported parameters, see the [`RunCreate`](../api-reference#RunCreate) reference documentation.

For example, to create a run for the project ID `1234` and to trigger a specific schedule name:

```bash
curl -H "Authorization: Bearer 5235783003799552|s+N8gAs72qbi90pFEv7yW/KBImTshRdBoVKjjFA7lD0=|1" -X POST -d '{ "scheduleName": "some_schedule" }' https://api.dataform.co/v1/project/1234/run
```

This will return a [RunGetResponse](../api-reference#/definitions/v1RunCreateResponse), that includes the created run's ID:

```json
{
  "id": "1029591293203"
}
```

## Getting run information

After creating a run, the status of the run can be checked with the [`RunGet`](../api-reference#RunGet) method.

This should be a `GET` request to the appropriate path, for example for project ID `1234` and a run ID `5678`:

```bash
curl -H "Authorization: Bearer 5235783003799552|s+N8gAs72qbi90pFEv7yW/KBImTshRdBoVKjjFA7lD0=|1" https://api.dataform.co/v1/project/1234/run/5678
```

This will return a [`RunGetResponse`](../api-reference#/definitions/v1RunGetResponse) such as:

```json
{
  "id": "5678",
  "status": "RUNNING",
  "runLogUrl": "https://app.dataform.co/#/1234/run/5678"
}
```
