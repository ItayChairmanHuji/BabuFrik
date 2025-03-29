from snsynth.pytorch.nn import PATECTGAN

from src.synthesizers import smartnoise_fixes
from src.synthesizers.smart_noise_synthesizer_base import SmartNoiseSynthesizerBase


class PATECTGANSynthesizer(SmartNoiseSynthesizerBase):
    def smart_noise_fixes(self) -> None:
        PATECTGAN.set_device = smartnoise_fixes.patectgan_set_device

    @property
    def smart_noise_name(self) -> str:
        return "patectgan"
