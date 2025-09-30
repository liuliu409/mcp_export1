from fastapi import Query
from services.glm_service import GLMService, GLMAnalysis
from controllers.base.base_controller import BaseController
from schemas.glm_schema import ImportDataAfterMapping, ImportValidateRequest, GLMRequest

class GLMController(BaseController):
    def __init__(self):
        super().__init__(prefix="/glm", tags=["GLM Shared Controller"])
        self.service = GLMService()
        self.analysis = GLMAnalysis()

        self.router.add_api_route("/mapping-columns/", self.mapping_columns, methods=["GET"])
        self.router.add_api_route("/glm-valid-data/", self.glm_valid_data, methods=["POST"])
        self.router.add_api_route("/glm-import-data-after-mapping/", self.glm_import_data_after_mapping, methods=["POST"])
        self.router.add_api_route("/glm-1wa/", self.glm_1wa, methods=["POST"])
        self.router.add_api_route("/glm-2wa/", self.glm_2wa, methods=["POST"])
        self.router.add_api_route("/glm-3wa/", self.glm_3wa, methods=["POST"])
        self.router.add_api_route("/glm-4wa/", self.glm_4wa, methods=["POST"])

    async def mapping_columns(self, url_file: str = Query(..., description="URL của file cần xử lý")):
        return await self.service.extract_mapping_columns(url_file)

    async def glm_valid_data(self, request: ImportValidateRequest):
        return await self.service.glm_valid_data(request)

    async def glm_import_data_after_mapping(self, request: ImportDataAfterMapping):
        return await self.service.glm_import_data_after_mapping(request)

    async def glm_1wa(self, request_body: GLMRequest):
        return await self.analysis.glm_1wa(request_body)

    async def glm_2wa(self, request_body: GLMRequest):
        return await self.analysis.glm_2wa(request_body)

    async def glm_3wa(self, request_body: GLMRequest):
        return await self.analysis.glm_3wa(request_body)

    async def glm_4wa(self, request_body: GLMRequest):
        return await self.analysis.glm_4wa(request_body)

glm_controller = GLMController()
router = glm_controller.router
