from nomad.config.models.plugins import NormalizerEntryPoint


class IVNormalizerEntryPoint(NormalizerEntryPoint):
    def load(self):
        from nomad_iv_sjis.normalizers.ivnormalizer import IVNormalizer
        return IVNormalizer(**self.dict())


ivnormalizer = IVNormalizerEntryPoint(
    name="IVNormalizer",
    description="Reads CP932/SJIS IV csv files and fills voltage/current arrays.",
)
