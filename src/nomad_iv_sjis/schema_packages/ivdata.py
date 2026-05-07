import io
import re
import pandas as pd
from nomad.metainfo import Quantity, Section, SchemaPackage
from nomad.datamodel.data import EntryData
from nomad.metainfo.elasticsearch_extension import Elasticsearch


m_package = SchemaPackage()


def _to_float(value):
    """Convert various numeric string representations to float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if s == '':
        return None
    # normalize unicode minus and remove surrounding spaces
    s = s.replace('−', '-')
    try:
        return float(s)
    except ValueError:
        # fallback: extract first float-like token
        m = re.search(r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?', s)
        return float(m.group(0)) if m else None


class IVData(EntryData):
    """
    Entry type for IV csv data with metadata header and IV curve.

    Assumed CSV structure:
    - line 1: version,...
    - line 2: file,...
    - line 3: datetime,...,temperature,...
    - line 4: Vstart(V),...,Vstop(V),...,Count,...,Area(cm2),...
    - line 5: Voc(V),...,Isc(A),...,FF(%),...,Pmax(W),...,Eff(%),...,Vpm(V),...,Ipm(A),...
    - line 6: header V(V),I(A)
    - line 7+: IV data
    """
    m_def = Section()

    # raw file
    data_file = Quantity(
        type=str,
        description='Raw IV csv file inside the upload.',
        a_eln={'component': 'FileEditQuantity'},
    )

    # header metadata
    version = Quantity(type=str, description='Version / instrument header line.')
    source_file_path = Quantity(type=str, description='Original file path written in the CSV header.')
    measurement_datetime = Quantity(type=str, description='Measurement date/time string from CSV header.')
    temperature = Quantity(type=float, description='Temperature value from CSV header.')

    v_start = Quantity(type=float, description='Sweep start voltage [V].')
    v_stop = Quantity(type=float, description='Sweep stop voltage [V].')
    count = Quantity(type=int, description='Number of IV points.')
    area_cm2 = Quantity(type=float, description='Cell area [cm^2].')




    voc = Quantity(
        type=float,
        unit='V',
        description='Open-circuit voltage.',
        a_elasticsearch=[Elasticsearch()],
    )

    isc = Quantity(
        type=float,
        unit='A',
        description='Short-circuit current.',
        a_elasticsearch=[Elasticsearch()],
    )

    ff_percent = Quantity(
        type=float,
        description='Fill factor [%].',
        a_elasticsearch=[Elasticsearch()],
    )

    pmax = Quantity(
        type=float,
        unit='W',
        description='Maximum power.',
        a_elasticsearch=[Elasticsearch()],
    )

    eff_percent = Quantity(
        type=float,
        description='Efficiency [%].',
        a_elasticsearch=[Elasticsearch()],
    )

    vpm = Quantity(
        type=float,
        unit='V',
        description='Voltage at maximum power.',
        a_elasticsearch=[Elasticsearch()],
    )

    ipm = Quantity(
        type=float,
        unit='A',
        description='Current at maximum power.',
        a_elasticsearch=[Elasticsearch()],
    )



    # IV arrays
    voltage = Quantity(
        type=float,
        shape=['*'],
        description='Voltage values parsed from the CSV column V(V).',
    )
    current = Quantity(
        type=float,
        shape=['*'],
        description='Current values parsed from the CSV column I(A).',
    )

    def normalize(self, archive, logger):

        """Read a CP932/SJIS IV csv file, parse metadata and IV arrays."""
        super().normalize(archive, logger)

        # 1. まず self.data_file から local variable を初期化する
        data_file = self.data_file

        # 2. data_file が未指定なら、今は明示エラーにする
        #    自動探索は NOMAD processing context によって raw file list が取れず不安定だったため
        if not data_file:
            raise ValueError(
                'data_file is not specified. '
                'Please specify the CSV file name in the archive.yaml, e.g. '
                'data_file: L303-M13_0min_rvs_re.csv'
            )

        # 3. 実際に使うファイル名を archive に保存する
        data_file = str(data_file)
        self.data_file = data_file

        logger.info('Using IV CSV file.', data_file=self.data_file)

        try:
            # Read raw bytes to avoid text-reader encoding conflicts.
            with archive.m_context.raw_file(self.data_file, 'rb') as f:
                raw = f.read()

            text = raw.decode('cp932')
            lines = text.splitlines()

            if len(lines) < 6:
                logger.warning('CSV does not contain enough lines.', file=self.data_file, n_lines=len(lines))
                return

            # line 1: version,...
            row0 = next(pd.read_csv(io.StringIO(lines[0]), header=None).itertuples(index=False), None)
            if row0 and len(row0) >= 2:
                self.version = str(row0[1]).strip() if row0[1] is not None else None

            # line 2: file,...
            row1 = next(pd.read_csv(io.StringIO(lines[1]), header=None).itertuples(index=False), None)
            if row1 and len(row1) >= 2:
                self.source_file_path = str(row1[1]).strip() if row1[1] is not None else None

            # line 3: datetime,...,temperature,...
            row2 = pd.read_csv(io.StringIO(lines[2]), header=None).iloc[0].tolist()
            for i in range(0, len(row2) - 1, 2):
                key = str(row2[i]).strip()
                val = row2[i + 1]
                if key == 'datetime':
                    self.measurement_datetime = str(val).strip() if val is not None else None
                elif key == 'temperature':
                    self.temperature = _to_float(val)

            # line 4: Vstart(V),...,Vstop(V),...,Count,...,Area(cm2),...
            row3 = pd.read_csv(io.StringIO(lines[3]), header=None).iloc[0].tolist()
            map_line4 = {
                'Vstart(V)': 'v_start',
                'Vstop(V)': 'v_stop',
                'Count': 'count',
                'Area(cm2)': 'area_cm2',
            }
            for i in range(0, len(row3) - 1, 2):
                key = str(row3[i]).strip()
                val = row3[i + 1]
                attr = map_line4.get(key)
                if attr:
                    if attr == 'count':
                        fv = _to_float(val)
                        setattr(self, attr, int(fv) if fv is not None else None)
                    else:
                        setattr(self, attr, _to_float(val))

            # line 5: Voc(V),...,Isc(A),...,FF(%),...,Pmax(W),...,Eff(%),...,Vpm(V),...,Ipm(A),...
            row4 = pd.read_csv(io.StringIO(lines[4]), header=None).iloc[0].tolist()
            map_line5 = {
                'Voc(V)': 'voc',
                'Isc(A)': 'isc',
                'FF(%)': 'ff_percent',
                'Pmax(W)': 'pmax',
                'Eff(%)': 'eff_percent',
                'Vpm(V)': 'vpm',
                'Ipm(A)': 'ipm',
            }
            for i in range(0, len(row4) - 1, 2):
                key = str(row4[i]).strip()
                val = row4[i + 1]
                attr = map_line5.get(key)
                if attr:
                    setattr(self, attr, _to_float(val))

            # line 6+: IV table with header V(V),I(A)
            df = pd.read_csv(io.StringIO('\n'.join(lines[5:])))
            if 'V(V)' not in df.columns or 'I(A)' not in df.columns:
                logger.warning('Expected columns not found in IV table.', columns=list(df.columns))
                return

            self.voltage = pd.to_numeric(df['V(V)'], errors='coerce').dropna().astype(float).tolist()
            self.current = pd.to_numeric(df['I(A)'], errors='coerce').dropna().astype(float).tolist()

            logger.info(
                'IV csv parsed successfully.',
                n_points=len(self.voltage),
                file=self.data_file,
                voc=self.voc,
                isc=self.isc,
                ff=self.ff_percent,
                eff=self.eff_percent,
            )

        except Exception as e:
            logger.error(
                'Failed to parse IV csv file.',
                file=self.data_file,
                exc_info=e,
            )
            raise




from pathlib import Path


def _find_csv_in_same_upload(archive, logger):
    """
    ####### これは使ってない ######
    
    NOMAD upload 内から IV CSV 候補を探す。

    優先順位:
    1. mainfile と同じディレクトリの CSV
    2. upload 全体の CSV

    Returns:
        str: NOMAD raw_file() に渡せる upload 内相対パス

    Raises:
        ValueError: CSV が見つからない、または複数あり一意に決められない場合
    """

    # archive.metadata.mainfile は、多くの場合 test_iv.archive.yaml の upload 内パス
    mainfile = getattr(archive.metadata, 'mainfile', None)
    main_dir = str(Path(mainfile).parent) if mainfile else '.'
    if main_dir == '.':
        main_dir = ''

    # NOMAD のバージョン差を吸収するため、候補取得を少し防御的に書く
    raw_files = []

    # 方法 1: m_context に raw_file_manifest がある場合
    if hasattr(archive.m_context, 'raw_file_manifest'):
        manifest = archive.m_context.raw_file_manifest()
        if isinstance(manifest, dict):
            raw_files = list(manifest.keys())
        else:
            raw_files = list(manifest)

    # 方法 2: upload_files.raw_file_manifest() がある場合
    elif hasattr(archive.m_context, 'upload_files') and hasattr(
        archive.m_context.upload_files, 'raw_file_manifest'
    ):
        manifest = archive.m_context.upload_files.raw_file_manifest()
        if isinstance(manifest, dict):
            raw_files = list(manifest.keys())
        else:
            raw_files = list(manifest)

    else:
        raise ValueError(
            'Could not list raw files in upload. '
            'Please specify data_file explicitly.'
        )

    csv_files = [
        f for f in raw_files
        if f.lower().endswith('.csv')
    ]

    if not csv_files:
        raise ValueError(
            'No CSV file was found in the upload. '
            'Please upload an IV CSV file or specify data_file.'
        )

    # mainfile と同じディレクトリの CSV を優先
    if main_dir:
        same_dir_csv_files = [
            f for f in csv_files
            if str(Path(f).parent) == main_dir
        ]
    else:
        same_dir_csv_files = [
            f for f in csv_files
            if str(Path(f).parent) in ('', '.')
        ]

    candidates = same_dir_csv_files or csv_files

    if len(candidates) == 1:
        logger.info('Automatically selected CSV file', data_file=candidates[0])
        return candidates[0]

    raise ValueError(
        'Multiple CSV files were found in the upload, '
        'and the IV CSV file could not be selected automatically: '
        f'{candidates}. Please specify data_file explicitly.'
    )

m_package.__init_metainfo__()
