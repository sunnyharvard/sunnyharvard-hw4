# HW4 — API Prototype (county_data)

Deployed on **Render**. Flask app + SQLite database (read-only).

## Live Endpoint

- **Base URL:**  
  [`https://sunnyharvard-hw4.onrender.com`](https://sunnyharvard-hw4.onrender.com)  
  Note: Please click on the link and wait for the application to wake up before sending requests

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
- **405**: wrong method (GET, etc.)

---

## Example cURL

```bash
# Happy path
curl -s -H 'content-type: application/json' \
  -d '{"zip":"02138","measure_name":"Adult obesity"}' \
  https://sunnyharvard-hw4.onrender.com/county_data | jq

# Teapot
curl -s -H 'content-type: application/json' \
  -d '{"zip":"02138","measure_name":"Adult obesity","coffee":"teapot"}' \
  https://sunnyharvard-hw4.onrender.com/county_data

# Bad request (missing keys)
curl -s -H 'content-type: application/json' \
  -d '{}' \
  https://sunnyharvard-hw4.onrender.com/county_data
