# RAP API Developer Documentation

## OpenAPI

We use the [OpenAPI](https://www.openapis.org/) [specification](https://spec.openapis.org/oas/latest.html#openapi-specification) to describe the RAP API endpoints.

The OpenAPI specification is described in a yaml file, found at [controller/webapp/api_spec/openapi.yaml](openapi.yaml). Each RAP API endpoint has an entry under `paths:` which
describes allowed methods, parameters, and expected responses for each method. It also allows
description and summary information for documentation.

See the OpenAPI docs for details of [how to describe endpoints with paths](https://learn.openapis.org/specification/paths.html).


### Validate the api spec

```
just validate-api-spec
```
This will validate the [api spec file](openapi.yaml) and report any errors.

You can also use the [Swagger editor](https://editor.swagger.io/); import the api spec file
(or copy and paste its contents) into the editor, and it will highlight any errors (note that
at the time of writing, the editor currently expects a file in openapi version 3.0.x and so
will show an error for the first line.) The editor also lets you visualise documentation of the endpoints generated from the spec.


### View the api spec as json in a browser

The spec can be viewed as json at `/api_spec.json`

<https://controller.opensafely.org/controller/v1/api_spec.json>


### Using examples

[Examples](https://learn.openapis.org/specification/docs.html#adding-examples) can be added to the
api spec file. These will be incorparated into generated api docs, and are also used by [automated
testing tools to produce example test cases](#using-examples-from-the-api-spec).


## Testing

### Schemathesis

[Schemathesis](https://schemathesis.readthedocs.io/) is a testing library that uses [hypothesis](https://hypothesis.readthedocs.io/) to generate test cases from the
OpenAPI spec.

#### Running schemathesis tests from the command line

We can run the schemathesis tests with the schemathesis CLI in 2 ways:
1. Against a locally running development server
2. Against a server running in a docker container


##### Run tests against a locally running development server

  First run the dev server:
  ```
  just run-app
  ```

  Then in a separate terminal:
  ```
  just schemathesis
  ```

##### Run tests against a server running in docker

  ```
  just test-api-spec
  ```


##### To test a single endpoint:

```
just test-api-spec --include-path /backend/status/
```

##### To record the test cases
This is useful if you're getting an error and want to check the details
of the test case that generated it:
```
just test-api-spec --report vcr
```
This records the test cases as yaml intended for use with VCR[^1].


#### Schemathesis unit tests

We also have [unit tests](tests/controller/webapp/test_api_spec.py) that run using schemathesis,
testing against a test server using the `live_server` fixture.

These allow us to setup test data to more easily test the happy paths for endpoints that
create/modify existing data.

#### Using examples from the API spec

If examples (schema examples or parameter examples) are present in the API spec document,
schemathesis will attempt to use them to generate hypothesis examples. We can use this to
ensure that tests are run against values that we expect as well as generated hypothesis
data.

e.g. for an an endpoint with a requestBody schema with `backend` and `id` parameters, we can specify
examples for `backend` as test, tpp and emis (our default backends):
```
paths:
...
  /rap/my-endpoint/:
      post:
        description: My test endpoint.
        requestBody:
          required: true
          content:
            application/json:
              schema:
                type: object
                required:
                  - backend
                  - id
                properties:
                  backend:
                    type: string
                  id:
                    type: string
              examples:
                test_backend:
                  value:
                    backend: test
                    id: 1234567
                tpp_backend:
                  value:
                    backend: tpp
                    id: 1234567
                emis_backend:
                  value:
                    backend: emis
                    id: 1234567
```

Schemathesis will generate examples that use the values from each of the schema exampkes we provided.
We can then set up our config in the tests so that e.g. the `test` backend is valid and authorised in the test call, the `tpp` backend is valid but not authorised, and the `emis` backend is invalid.

## Documentation

```
just generate-api-docs
```

We use [redoc](https://github.com/Redocly/redoc) to automatically generate documentation from the
api spec. The above just command assumes that you already have `node` installed.


API docs are saved to [controller/webapp/api_spec/api_docs.html](controller/webapp/api_spec/api_docs.html)
and are served at /api-docs


To view docs locally, run the dev server:
```
just run-app
```

And then navigate to http://localhost:3000/controller/v1/api-docs/

Production docs can be found at: https://controller.opensafely.org/controller/v1/api-docs/


### Regenerating docs

Whenever a change is made to the OpenAPI spec file, the API docs will need to be regenerated and
committed.

You can check whether docs are up to date with:
```
just check-api-docs
```

This is also run in a GitHub action workflow as part of the CI build, and will fail if the docs
are out of date.


### Summary and description in the API spec

Paths in the API spec can have both a `summary` - a short description - and a `description` field.

Almost every other object in the API spec can have a `description`.

[Summary and description](https://learn.openapis.org/specification/docs.html#providing-documentation-and-examples) fields will included in the generated documentation.

Description fields can include markdown formatting.


## Process for adding a new endpoint

See [How to add a new endpoint](how_to_add_a_new_endpoint.md) for an example.

[^1]: Note: we're not using VCR - either the original [ruby](https://github.com/vcr/vcr) or the
[python](https://vcrpy.readthedocs.io/) version; this output is just the most readable of the
report output options.
