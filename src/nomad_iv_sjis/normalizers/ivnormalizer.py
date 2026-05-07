import pandas as pd
from nomad.normalizing import Normalizer


class IVNormalizer(Normalizer):
    """
    Read a CP932/SJIS IV csv file and fill IVData.voltage/current.

    Assumed CSV structure:
    - lines 1-5: metadata
    - line 6: header V(V),I(A)
    - line 7+: data
    """

    def normalize(self, logger):
        super().normalize(logger)

        archive = self.archive
        data = getattr(archive, "data", None)
        if data is None:
            logger.debug("No archive.data found, skipping normalizer.")
            return

        if not hasattr(data, "data_file") or not data.data_file:
            logger.debug("No data_file set, skipping normalizer.")
            return

        try:
            with archive.m_context.raw_file(data.data_file) as f:
                df = pd.read_csv(
                    f,
                    encoding="cp932",
                    skiprows=5
                )

            if "V(V)" not in df.columns or "I(A)" not in df.columns:
                logger.warning(
                    "Expected columns not found.",
                    columns=list(df.columns)
                )
                return

            data.voltage = df["V(V)"].astype(float).tolist()
            data.current = df["I(A)"].astype(float).tolist()

            logger.info(
                "IV csv parsed successfully.",
                n_points=len(data.voltage),
                file=data.data_file
            )

        except Exception as e:
            logger.error(
                "Failed to parse IV csv file.",
                file=data.data_file,
                exc_info=e
            )
            raise
