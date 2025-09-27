# HW4 — API Prototype (county_data)

Deployed on Vercel. Serverless Python function + SQLite (read-only).

## Endpoint

- **POST** `/county_data`  
  Content-Type: `application/json`

### Required body keys

- `zip`: 5-digit ZIP code (string or number)
- `measure_name`: one of
  - Violent crime rate
  - Unemployment
  - Children in poverty
  - Diabetic screening
  - Mammography screening
  - Preventable hospital stays
  - Uninsured
  - Sexually transmitted infections
  - Physical inactivity
  - Adult obesity
  - Premature Death
  - Daily fine particulate matter

### Special behavior

- If the body includes `"coffee": "teapot"`, returns **418**.

### Error codes

- **400**: missing/invalid inputs
- **404**: valid inputs but no matching data
- **405**: non-POST

## Example cURL

```bash
curl -s -H 'content-type: application/json' \
  -d '{"zip":"02138","measure_name":"Adult obesity"}' \
  https://YOUR-DEPLOYMENT.vercel.app/county_data | jq
