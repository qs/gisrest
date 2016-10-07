"""
Web application for rest api service to get nearest points and user management.

Get neighbours:
    GET http://localhost:8000/api/v1/locate?lat=100&lng=100&radius=10&limit=10
    Default values:
        lat = 0, lng = 0, radius = 1, limit = 1

User management:
    Create a user:
        curl -X PUT -d "lat=0&lng=0&name=user1" http://localhost:8000/api/v1/user
    Delete the user:
        curl -X DELETE http://localhost:8000/api/v1/user/13
    Update the user:
        curl -X POST -d "lat=1&lng=1" http://localhost:8000/api/v1/user/13
    Get user data:
        curl http://localhost:8000/api/v1/user/13

"""
import asyncio
from aiohttp import web
import aiopg
from decimal import Decimal

import settings


class App:
    async def locate_handler(self, request):
        """
        Async handler for executing query.
        Example URL:
            GET /api/v1/locate?lat=100&lng=100&radius=10&limit=10
        """
        lat = Decimal(request.GET.get('lat', 0))
        lng = Decimal(request.GET.get('lng', 0))
        radius = Decimal(request.GET.get('radius', 1))
        limit = Decimal(request.GET.get('limit', 1))

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT id, name, ST_X(location), ST_Y(location)
                    FROM users
                    WHERE ST_DWithin(location,
                                     ST_SetSRID(ST_MakePoint(%s, %s), %s), %s)
                    LIMIT %s""", (lat, lng, radius, settings.SRID, limit))
                users = []
                async for row in cur:
                    users.append(dict(zip(settings.USER_HEADER, row)))
        return web.json_response({'users': users})

    async def get_user_handler(self, request):
        """
        Get user data.
        Example:
            curl -X GET http://localhost:8000/api/v1/user/13
        """
        user_id = Decimal(request.match_info.get('user_id', 0))
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT id, name, ST_X(location), ST_Y(location)
                    FROM users WHERE id=%s""", (user_id, ))
                user_data = await cur.fetchone()
        user = dict(zip(settings.USER_HEADER, user_data))
        return web.json_response({'user': user})

    async def update_user_handler(self, request):
        """
        Update user.
        Request must contains both lat and lng in case of point update.
        Example:
            curl -X POST -d "lat=1&lng=1" http://localhost:8000/api/v1/user/13
        """
        data = await request.post()
        user_id = Decimal(request.match_info.get('user_id', 0))
        lat = Decimal(data['lat']) if 'lat' in data else None
        lng = Decimal(data['lng']) if 'lat' in data else None
        name = data.get('name')

        if not user_id:
            return web.json_response({'result': 'Invalid user id'})
        if not name and not (lat and lng):
            return web.json_response({'result': 'Invalid update data'})

        update_fields = []
        query_params = {'user_id': user_id, 'srid': settings.SRID}
        if lat and lng:
            update_fields.append(
                "location = ST_GeomFromText(%(point)s, %(srid)s)")
            query_params['point'] = 'POINT({0} {1})'.format(lat, lng)
        if name:
            update_fields.append("name = %(name)s")
            query_params['name'] = name
        query = "UPDATE users SET {fields} WHERE id=%(user_id)s"\
            .format(fields=', '.join(update_fields))

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, query_params)
        return web.json_response({'result': 'success'})

    async def create_user_handler(self, request):
        """
        Add user handler.
        Example:
            curl -X PUT -d "lat=0&lng=0&name=user1" \
            http://localhost:8000/api/v1/user
        """
        lat = Decimal(request.match_info.get('lat', 0))
        lng = Decimal(request.match_info.get('lng', 0))
        name = request.match_info.get('lng', 'user')
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """INSERT INTO users (name, location)
                        VALUES (%s, ST_GeomFromText(%s, %s))
                        RETURNING id""",
                    (name, 'POINT({0} {1})'.format(lat, lng), settings.SRID))
                user_ids = await cur.fetchone()
        return web.json_response({'user_id': user_ids[0]})

    async def delete_user_handler(self, request):
        """
        Remove user from db.
        Example:
            curl -X DELETE http://localhost:8000/api/v1/user/13
        """
        user_id = Decimal(request.match_info.get('user_id', 0))
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """DELETE FROM users WHERE id=%s""", (user_id, ))
        return web.json_response({'result': 'success'})

    async def init(self, loop):
        """
        Init connection to db (creating pool).
        Creation of web app with routes
        """
        self.pool = await aiopg.create_pool(settings.DSN)
        app = web.Application()

        prefix = '/api/v1'
        app.router.add_route('GET', prefix + '/locate',
                             self.locate_handler)
        app.router.add_route('GET', prefix + '/user/{user_id}',
                             self.get_user_handler)
        app.router.add_route('POST', prefix + '/user/{user_id}',
                             self.update_user_handler)
        app.router.add_route('PUT', prefix + '/user',
                             self.create_user_handler)
        app.router.add_route('DELETE', prefix + '/user/{user_id}',
                             self.delete_user_handler)

        server = await loop.create_server(app.make_handler(), '0.0.0.0', 8000)
        print('serving on', server.sockets[0].getsockname())
        return server

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = App()
    loop.run_until_complete(app.init(loop))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

