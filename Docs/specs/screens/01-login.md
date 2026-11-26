# Screen: Login

**Route:** `/login`  
**Component:** `src/app/login/page.tsx`

## Purpose
Entry point for all users. Authenticates against the QueryAstra backend and stores the JWT
in `AuthContext`.

## API calls
- `POST /api/v1/auth/login` — `{username, password}` → `{access_token, token_type}`

## UI
- Username and password fields
- Submit button with loading state
- Error banner on invalid credentials
- Link to reset password (`/reset-password`)

## Behavior
- On success: stores token, redirects to `/instances`
- On failure: inline error chosen by the cause (`loginErrorKey` in `src/app/login/_content.tsx`):

  | Response | Message key (`Login.*`) | pt-BR |
  |---|---|---|
  | 4xx (e.g. 401) | `error` | Usuário ou senha incorretos |
  | 429 (10 failures / 10 min per IP) | `errorTooManyAttempts` | Muitas tentativas sem sucesso… |
  | network error / 5xx (backend down or restarting) | `errorUnavailable` | Não foi possível conectar ao servidor… |

  A backend outage must never be reported as a wrong password.
- If already authenticated, redirects immediately to `/instances`

## Notes
`AuthContext` starts with `isLoading: true` when a token already exists in storage,
preventing a flash to the login page on hard-reload.
