import csv
import dataclasses
import os
import typing as t

import inflection
import pymysql

from AzurStats.azurstats import AzurStats, AzurStatsOpsi
from AzurStats.config.config import CONFIG
from AzurStats.database.item_info import ItemInfo
from AzurStats.database.output import ResultOutput
from module.config.utils import deep_get, read_file, write_file
from module.device.method.utils import remove_prefix
from module.logger import logger
from module.map.map_grids import SelectedGrids
from module.ocr.ocr import Ocr

Ocr.SHOW_LOG = False


@dataclasses.dataclass
class DataImage:
    imgid: str
    path: str
    device_id: str = ''
    genre: str = ''
    combat_count: int = 0


class AzurStatsDatabase(ItemInfo, ResultOutput):
    # Numbers of images processed on each batch
    BATCH_SIZE = 100

    def __init__(self):
        self.image_folder = str(deep_get(CONFIG, 'Folder.images'))
        self.database_config = deep_get(CONFIG, 'Database')

    def abspath(self, path):
        """
        Args:
            path (str, DataImage): Such as `/imgs/2021/07/d91b7b6637c400ee.png`

        Returns:
            str: Path to image, such as `F:/azurstats.lyoko.io/imgs/2021/07/d91b7b6637c400ee.png`
        """
        if isinstance(path, DataImage):
            path = path.path
        path = remove_prefix(path, '/imgs/')
        return os.path.abspath(os.path.join(self.image_folder, path)).replace('\\', '/')

    def get_total_updates(self) -> int:
        """
        Get the amount of images that haven't been parsed
        """
        sql = f"""
        SELECT COUNT(*)
        FROM azurstat.img_images a
        WHERE (
            SELECT COUNT(*)
            FROM azurstat_data.parse_records b
            WHERE a.imgid = b.imgid
        ) = 0
        """
        connection = pymysql.connect(**self.database_config)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                return rows[0][0]
        finally:
            connection.close()

    def query(self, sql: str, data_class: dataclasses.dataclass) -> SelectedGrids:
        """
        Args:
            sql:
            data_class:

        Returns:
            SelectedGrids[data_class()]
        """
        connection = pymysql.connect(**self.database_config)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                return SelectedGrids([data_class(*row) for row in rows])
        finally:
            connection.close()

    def select_batch_images(self) -> SelectedGrids(DataImage):
        """
        Get list a batch of images to process
        """
        sql = f"""
        SELECT a.imgid, a.path, a.device_id, a.genre, a.combat_count
        FROM azurstat.img_images a
        LEFT JOIN azurstat_data.parse_records b ON a.imgid = b.imgid
        WHERE ISNULL(b.imgid)
        LIMIT {AzurStatsDatabase.BATCH_SIZE}
        """
        # sql = f"""
        # SELECT imgid, path
        # FROM azurstat.img_images a
        # WHERE (
        #     SELECT COUNT(*)
        #     FROM azurstat_data.parse_records b
        #     WHERE a.imgid = b.imgid
        # ) = 0
        # ORDER BY id ASC
        # LIMIT {AzurStatsDatabase.BATCH_SIZE}
        # """
        return self.query(sql, DataImage)

    def record_to_json(self, record: SelectedGrids, file: str = None) -> t.Dict:
        """
        Convert a SelectGrids object to json.
        If `file`, write json into it.
        """
        out = {}
        for index, data in enumerate(record):
            out[index] = dataclasses.asdict(data)

        if file is not None:
            write_file(file, data=out)
            logger.info(f'Record wrote into {file}')
        return out

    def record_to_csv(self, record: SelectedGrids, file: str = None, encoding='utf-8') -> t.List[t.List[t.Any]]:
        """
        Convert a SelectGrids object to csv files.
        If `file`, write csv into it.
        """

        def to_fields(row):
            if isinstance(row, dict):
                return list(row.keys())
            else:
                return [column.name for column in dataclasses.fields(row)]

        def to_value(row):
            if isinstance(row, dict):
                return list(row.values())
            else:
                return list(dataclasses.astuple(row))

        out = []
        if record:
            out += [to_fields(record[0])]
        out += [to_value(r) for r in record]

        if file is not None:
            with open(file, 'w', newline='', encoding=encoding) as csv_file:
                writer = csv.writer(csv_file)
                writer.writerows(out)
            logger.info(f'Record wrote into {file}')

        return out

    def record_from_json(self, record: [SelectedGrids, str], data_class: dataclasses.dataclass) -> SelectedGrids:
        if isinstance(record, str):
            record = read_file(record)
        out = [data_class(**data) for data in record.values()]
        return SelectedGrids(out)

    @staticmethod
    def _insert_data(data_list: t.List, cursor: pymysql.connections.Cursor, metadata: dict = None):
        # First row of data
        first = next(iter(data_list), None)
        if first is None:
            return

        columns = [field.name for field in dataclasses.fields(first)]
        # ResearchItem
        scene = remove_prefix(first.__class__.__name__, 'Data')
        # research_item
        table = inflection.underscore(scene)

        if metadata:
            if table == 'parse_records':
                columns.extend(['device_id', 'genre'])
            else:
                columns.extend(['device_id', 'genre', 'combat_count'])
            
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        sql = f"""
        INSERT INTO `azurstat_data`.`{table}` ({columns_str})
        VALUES ({placeholders})
        """
        
        batch = []
        for data in data_list:
            row = list(dataclasses.astuple(data))
            if metadata:
                device_id, genre, combat_count = metadata.get(data.imgid, ('', '', 0))
                if table == 'parse_records':
                    row.extend([device_id, genre])
                else:
                    row.extend([device_id, genre, combat_count])
            batch.append(row)

        logger.info(sql)
        rows = cursor.executemany(sql, batch)
        logger.info(f'Rows inserted: {rows}')

    def insert_azurstats(self, azurstats: AzurStats, images: SelectedGrids = None):
        metadata = {}
        if images is not None:
            for img in images:
                metadata[img.imgid] = (img.device_id, img.genre, img.combat_count)
                
        connection = pymysql.connect(**self.database_config)
        try:
            with connection.cursor() as cursor:
                for attr in azurstats.all_data_type:
                    print(attr, len(getattr(azurstats, attr)))
                    self._insert_data(getattr(azurstats, attr), cursor=cursor, metadata=metadata)
                connection.commit()
        finally:
            connection.close()

    def update(self):
        total = self.get_total_updates()
        processed = 0
        while 1:
            logger.hr(f'Execute {processed}/{total}', level=1)
            images = self.select_batch_images()
            images_abs = [self.abspath(image) for image in images]

            # Parse
            azurstats = AzurStatsOpsi(images_abs)
            self.insert_azurstats(azurstats, images=images)

            processed += len(images)
            if processed >= total:
                break


if __name__ == '__main__':
    self = AzurStatsDatabase()
    self.update()
