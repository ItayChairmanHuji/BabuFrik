from mbi import GraphicalModel
from snsynth.mst import MSTSynthesizer as SNMSTSynthesizer

from src.synthesizers import smartnoise_fixes
from src.synthesizers.smart_noise_synthesizer_base import SmartNoiseSynthesizerBase


class MSTSynthesizer(SmartNoiseSynthesizerBase):
    def smart_noise_fixes(self) -> None:
        SNMSTSynthesizer.compress_domain = smartnoise_fixes.mst_compress_domain
        GraphicalModel.synthetic_data = smartnoise_fixes.graphical_model_synthetic_data

    @property
    def smart_noise_name(self) -> str:
        return "mst"
