import io
import re
import pandas as pd
from nomad.metainfo import Quantity, Section, SchemaPackage
from nomad.datamodel.data import EntryData
from nomad.metainfo.elasticsearch_extension import Elasticsearch
from nomad.datamodel.metainfo.plot import PlotSection, PlotlyFigure #, PlotlyGraphObject

from nomad.units import ureg

from nomad.datamodel.results import (
    Results,
    Properties,
    OptoelectronicProperties,
    SolarCell,
)


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

"""
class IVData(EntryData, PlotSection):
    m_def = Section(
        a_plot=[
            PlotlyGraphObject(
                label='IV curve',
                graph_object={
                    'data': [
                        {
                            'type': 'scatter',
                            'mode': 'lines+markers',
                            'name': 'IV curve',s
                            'x': '#voltage',
                            'y': '#current',
                        }
                    ],
                    'layout': {
                        'title': 'IV curve',
                        'xaxis': {'title': 'Voltage / V'},
                        'yaxis': {'title': 'Current / A'},
                    },
                },
            )
        ]
    )

    # 以下は既存 Quantity 定義
    # data_file = Quantity(...)
    # voltage = Quantity(type=float, shape=['*'])
    # current = Quantity(type=float, shape=['*'])
"""
class IVData(EntryData, PlotSection):
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


    
    jsc_mA_cm2 = Quantity(
        type=float,
        description='Short-circuit current density in mA/cm^2.'
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


            # mA/cm2 で扱えるように jsc_mA_cm2 という項目も作る.            
            if self.isc is not None and self.area_cm2 is not None:
                isc = self._to_quantity(self.isc, ureg.ampere)
                area = self._to_quantity(self.area_cm2, ureg.centimeter ** 2)

                if area.magnitude != 0:
                    jsc_mA_cm2 = (isc / area).to(
                        ureg.milliampere / (ureg.centimeter ** 2)
                    )
                    self.jsc_mA_cm2 = jsc_mA_cm2.magnitude



            self._copy_to_solar_cell_results(archive, logger)
            
            self.figures = [
                PlotlyFigure(
                    label='IV curve',
                    figure={
                        'data': [
                            {
                                'type': 'scatter',
                                'mode': 'lines+markers',
                                'name': self.data_file or 'IV curve',
                                'x': list(self.voltage),
                                'y': list(self.current),
                            }
                        ],
                        'layout': {
                            'title': {'text': self.data_file or 'IV curve'},
                            'xaxis': {'title': {'text': 'Voltage / V'}},
                            'yaxis': {'title': {'text': 'Current / A'}},
                        },
                    },
                )
            ]

            
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
        
    def _to_quantity(self, value, unit):
        """Return value as a Pint quantity with the given unit."""
        if value is None:
            return None

        # value がすでに Pint Quantity の場合
        if hasattr(value, "to"):
            return value.to(unit)

        # value が plain float/int の場合
        return float(value) * unit


    def _to_float_magnitude(self, value):
        """Return dimensionless magnitude from float or Pint quantity."""
        if value is None:
            return None

        if hasattr(value, "magnitude"):
            return float(value.magnitude)

        return float(value)


    def _copy_to_solar_cell_results(self, archive, logger):
        """Copy IV parameters to NOMAD standard Solar Cells results."""

        if archive.results is None:
            archive.results = Results()

        if archive.results.properties is None:
            archive.results.properties = Properties()

        if archive.results.properties.optoelectronic is None:
            archive.results.properties.optoelectronic = OptoelectronicProperties()

        if archive.results.properties.optoelectronic.solar_cell is None:
            archive.results.properties.optoelectronic.solar_cell = SolarCell()

        solar_cell = archive.results.properties.optoelectronic.solar_cell

        # efficiency: unit=None
        if self.eff_percent is not None:
            solar_cell.efficiency = self._to_float_magnitude(self.eff_percent)

        # fill_factor: unit=None
        if self.ff_percent is not None:
            solar_cell.fill_factor = self._to_float_magnitude(self.ff_percent)

        # open_circuit_voltage: unit=volt
        if self.voc is not None:
            voc = self._to_quantity(self.voc, ureg.volt)
            solar_cell.open_circuit_voltage = voc.magnitude

        # short_circuit_current_density: unit=ampere / meter ** 2
        if (
            self.isc is not None
            and self.area_cm2 is not None
        ):
            isc = self._to_quantity(self.isc, ureg.ampere)
            area = self._to_quantity(self.area_cm2, ureg.centimeter ** 2)

            if area.magnitude != 0:
                jsc = (isc / area).to(ureg.ampere / (ureg.meter ** 2))
                solar_cell.short_circuit_current_density = jsc.magnitude
        







m_package.__init_metainfo__()
