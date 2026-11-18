# Screen: Notification Center

**Component:** `src/components/notifications/NotificationBell.tsx`  
**Hook:** `src/hooks/useNotificationCenter.ts`  
**Location:** TopNav (bell icon, top-right)

## Purpose
Real-time notification inbox accessible from any page. Delivers alerts, slow-query
events and health events to users without requiring them to navigate away.

## API calls
- `GET /api/v1/notifications` — list notifications (paginated, latest first)
- `PATCH /api/v1/notifications/{id}/read` — mark single notification as read
- `PATCH /api/v1/notifications/read-all` — mark all notifications as read
- WebSocket `/api/v1/ws/live` — receives `notification` push events in real time

## TopNav bell button
- Shows `Bell` icon when unread count is 0; `BellRing` when > 0
- Red badge overlaid top-right of the icon: count capped at "99+"
- `aria-label` changes to include unread count when > 0
- Opens/closes the panel dropdown on click

## Notification panel (dropdown)
Rendered as `role="dialog"` anchored to the bell button. Width 384 px, max 100 vw − 2 rem.

**Header row:**
- Title "Notificações"
- "Marcar todas como lidas" button (disabled when 0 unread or marking in progress)

**List area** (max-height 24 rem, scrollable):
- Each item: coloured severity dot (red = critical, amber = warning, sky = info),
  title (bold if unread), description line, relative time ("há 3 min")
- Clicking an item: marks it as read (if unread), closes the panel, navigates to
  `notification.link` if it is a valid internal path

**States:**
- Loading: spinner text
- Error: error text in destructive colour
- Empty: centred "Nenhuma notificação" text

**Browser notifications footer** (shown when permission is not `denied` or `unsupported`
and browser notifications are not yet active):
- "Ativar notificações do navegador" link — calls `enableBrowserNotifications()`

## Close behaviour
Clicking outside the panel (pointer down outside container) or pressing Escape closes it.

## Severity colour map
| Severity | Dot colour |
|----------|-----------|
| critical | red-500 |
| warning  | amber-500 |
| info     | sky-500 |
