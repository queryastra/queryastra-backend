# Screen: Teams Admin

**Route:** `/settings/administracao/times`  
**Component:** `src/app/settings/administracao/times/page.tsx`  
**Access:** Admin only — non-admins are redirected to `/`

## Purpose
Create and manage teams. Each team groups users and grants them access to a set of
instances. Used to scope alert rules and instance visibility to a subset of users.

## API calls
- `GET /api/v1/auth/teams` — list teams with `memberIds` and `instanceIds`
- `POST /api/v1/auth/teams` — create team
- `PATCH /api/v1/auth/teams/{id}` — rename / update description
- `DELETE /api/v1/auth/teams/{id}` — delete team
- `POST /api/v1/auth/teams/{id}/members` — add member (body: `{ user_id }`)
- `DELETE /api/v1/auth/teams/{id}/members/{user_id}` — remove member
- `POST /api/v1/auth/teams/{id}/instances` — grant instance access
- `DELETE /api/v1/auth/teams/{id}/instances/{instance_id}` — revoke instance access
- `GET /api/v1/auth/users` — list users (to populate add-member select)
- `GET /api/v1/instances` — list instances (to populate add-instance select)

## Layout
Max width 3xl, single column, gap-6.

**Page header:**
- Subtitle text ("Gerencie equipes...")
- "Novo time" button (top-right of header row); hidden while the creation form is open

**New team form** (shown when "Novo time" is clicked):
- Name input (required, maxLength 128) + Description input (optional, maxLength 1000)
- 2-column grid layout
- Error message if 409 (name conflict) or generic create error
- "Criar" (disabled while pending or name empty) + "Cancelar" buttons

**Team list:** one `TeamRow` per team, vertically stacked.

## TeamRow
Collapsed view (always visible):
- Team name (bold) + member count badge + instance count badge
- Optional description (muted, below name)
- Inline edit mode: replaces name/description with text inputs; "Salvar" / "Cancelar"
- "Editar" link → activates inline edit
- "Gerenciar" / "Ocultar detalhes" toggle → expands/collapses the detail panel
- "Excluir" link (destructive) → `window.confirm` before DELETE

Expanded detail panel (2-column grid):
- **Members section**: chips for each member with × to remove; select + "Adicionar"
  for available users (users already in the team are excluded from the select)
- **Instances section**: chips for each granted instance with × to revoke; select +
  "Adicionar" for available instances (already granted instances are excluded)

## Invalidation
After any mutation the component invalidates:
- `["auth-teams"]`
- `["auth-users"]` (users screen shows team membership per user)
