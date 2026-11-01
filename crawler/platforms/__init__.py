"""Danh sách các sàn: tên sàn (khớp platforms.yaml, watchlist.csv) -> class Platform."""

from platforms.base import Platform
from platforms.cellphones import CellphonesPlatform
from platforms.fptshop import FptshopPlatform
from platforms.hoangha import HoanghaPlatform
from platforms.tgdd import TgddPlatform
from platforms.tiki import TikiPlatform

PLATFORM_CLASSES: dict[str, type[Platform]] = {
    cls.name: cls for cls in (TikiPlatform, CellphonesPlatform, HoanghaPlatform, FptshopPlatform, TgddPlatform)
}
