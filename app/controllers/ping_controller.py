from controllers.base.base_controller import BaseController
from fastapi import Request

class PingController(BaseController):
    def __init__(self):
        super().__init__(prefix="/ping", tags=["Ping Controller"])
        self.router.add_api_route("", self.ping, methods=["GET"])

    async def ping(self, request: Request) -> dict:
        return {"message": "Ping successfully", "url": str(request.url)}

ping_controller = PingController()
router = ping_controller.router
