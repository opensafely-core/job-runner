
## Process for adding a new endpoint

Example:

We want to add a new endpoint, at /my-endpoint/, which only accepts GET requests,
takes a `?foo=` query parameter, and returns a 200 JSON response with the value of `foo`.

### 1) First define the endpoint in the api spec yaml file:

```
  /my-endpoint/:
    summary: A new endpoint
    description: Description of my new endpoint
    security:
        - bearerAuth: []
    get:
      parameters:
        - name: foo
          in: query
          schema:
            type: string
          examples:
            ex1:
              value:
                foo: bar
      responses:
        200:
          description: All OK; foo is good
          content:
            application/json:
              schema:
                type: object
                properties:
                  result:
                    type: string
        401:
          $ref: "#/components/responses/401Unauthorized"
```

### 2) Validate the spec

Use the [Swagger editor](./RAP_API_DEVELOPERS.md#validate-the-api-spec) to validate the api spec.

### 3) Run the tests
```
just test-api-spec
```
We haven't written the endpoint yet, so this should fail.

```
...
====================================== FAILURES =====================================
_____________________________ GET /backend/my-endpoint/ _____________________________
1. Test Case ID: kzjicN

- Undocumented HTTP status code

    Received: 404
    Documented: 200

[404] Not Found:

    `<!DOCTYPE html>
    <html lang="en">
    <head>
      <meta http-equiv="content-type" content="text/html; charset=utf-8">
      <title>Page not found at /backend/my-endpoint/</title>
      <meta name="robots" content="NONE,NOARCHIVE">
      <style>
        html * { padding:0; margin:0; }
        body * { padding:10px 20px; }
        body * * { padding:0; }
        body { font-family: sans-serif; background:#eee; color:#000; }
        body > :where(header, main, footer) { border-bottom:1px solid #ddd; }
        h1 { font-weight:normal; margin-bottom:.4em; // Output truncated...`

Reproduce with:

    curl -X GET -H 'Authorization: [Filtered]' 'http://localhost:3030/backend/my-endpoint/?foo=foo'
...

```

### 4) Add the endpoint and view code:

In controller/webapp/urls.py
```
...
    path("my-endpoint/", rap_views.my_endpoint),
...
```

In controller/webapp/views/rap_views.py
```
def my_endpoint(request):
    # dummy auth check to match config in schemathesis.toml
    if request.headers.get("Authorization") != "token":
        return JsonResponse({"error": "no auth"}, status=401)
    foo = request.GET.get("foo")
    return JsonResponse({"result": foo})
```

### 5) Run tests again (just for the new endpoint)

```
just test-api-spec --include-path /my-endpoint/
```

This time we see a failure due to a method we haven't specified.
```
====================================== FAILURES =====================================
_________________________________ GET /my-endpoint/ _________________________________
1. Test Case ID: g9fzwy

- Unsupported method incorrect response

    Wrong status for unsupported method TRACE (got 200, expected 405)

[200] OK:

    `{"result": "foo"}`

Reproduce with:

    curl -X TRACE -H 'Authorization: [Filtered]' 'http://localhost:3030/my-endpoint/?foo=foo'
```

### 6) Update the view

Make the following changes:

- Add the `require_GET` decorator to our view to make Django reject other methods with a 405.
- Add the `csrf_exempt` decorator so that PUT attempts are rejected with the expected 405 and not a 403.
```
@csrf_exempt
@require_GET
def my_endpoint(request):
    if request.headers.get("Authorization") != "token":
        return JsonResponse({"error": "no auth"}, status=401)
    foo = request.GET.get("foo", "")
    return JsonResponse({"result": foo})
```

### 7) Run tests again

Tests should now pass!

```
Schemathesis v4.0.21
━━━━━━━━━━━━━━━━━━━━

 ✅  Loaded specification from controller/webapp/api_spec/openapi.yaml (in 0.19s)

     Base URL:         http://localhost:3030
     Specification:    Open API 3.1.0
     Operations:       1 selected / 3 total

 ✅  API capabilities:

     Supports NULL byte in headers:    ✓

 ✅  Examples (in 0.24s)

     ✅ 1 passed

 ✅  Coverage (in 0.93s)

     ✅ 1 passed

 ✅  Fuzzing (in 19.39s)

     ✅ 1 passed

====================================== SUMMARY ======================================

API Operations:
  Selected: 1/3
  Tested: 1

Test Phases:
  ✅ Examples
  ✅ Coverage
  ✅ Fuzzing
  ⏭  Stateful (not applicable)

Test cases:
  1216 generated, 1216 passed, 806 skipped

Seed: 168963956824987092624350218031285847507

============================= No issues found in 20.61s =============================
```
