"""Deliberately faulty sandbox implementation and its corrected counterpart."""


def sync_routes(payload, cache, now):
    # Atomic batch: one malformed record aborts refresh of every station route.
    staged = {}
    for vehicle in payload:
        route = vehicle['routes'][0]
        staged[vehicle['vehicle_id']] = {'route': route, 'expires_at': now + 600}
    cache.update(staged)


def sync_routes_fixed(payload, cache, now):
    skipped = []
    for vehicle in payload:
        routes = vehicle.get('routes', [])
        if not routes:
            skipped.append(vehicle['vehicle_id'])
            continue
        cache[vehicle['vehicle_id']] = {'route': routes[0], 'expires_at': now + 600}
    return skipped
