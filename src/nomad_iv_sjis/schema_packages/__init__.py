from nomad.config.models.plugins import SchemaPackageEntryPoint


class IVSchemaEntryPoint(SchemaPackageEntryPoint):
    def load(self):
        from nomad_iv_sjis.schema_packages.ivdata import m_package
        return m_package


ivschema = IVSchemaEntryPoint(
    name="IV Schema",
    description="Schema for IV CSV data with a raw file input.",
)
