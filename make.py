from dbtopy.field import Field


class Make(object):

    def __init__(self, n):
        if n == 1:
            self.write_mode = 'w'
        else:
            self.write_mode = 'a'

    def make_add_sql(self, table):
        sql = ''
        # field
        for (field_name, field) in table.field_list.items():

            sql += '\tADD COLUMN'
            sql += ' `' + field.name + '`'
            sql += ' ' + field.type

            if field.size != '' and int(field.size) > 0:
                sql += '(' + field.size + ')'

            if field.unsigned == 'true':
                sql += ' unsigned'

            if field.charset != '':
                if field.charset not in field.CHARSET_TYPE:
                    raise TypeError('charset error:' + field.charset)
                else:
                    sql += ' CHARACTER SET ' + field.charset

            if field.value:
                sql += ' DEFAULT \'' + field.param + '\''
            elif field.null == 'true':
                sql += ' DEFAULT NULL'
            else:
                sql += ' NOT NULL'

            if field.desc != '':
                sql += ' COMMENT \'' + field.desc + '\''

            sql += ',\n'

        # index
        if table.primary_key != '':
            sql += '\tADD PRIMARY KEY (' + self.deal_index(table.primary_key) + '),\n'

        for (index_name, index) in table.index_list.items():
            if index.unique == 'true':
                sql += '\tADD UNIQUE KEY `index_' + index.name.replace(',', '_') + '`'
            else:
                sql += '\tADD INDEX `index_' + index.name.replace(',', '_') + '`'

            sql += ' (' + self.deal_index(index.value, add_del=True) + '),\n'

        # default index
        sql += '\tADD UNIQUE KEY index_default_del(' + self.deal_index(table.primary_key, add_del=True) + ')'

        sql += '\n;\n'

        if table.split:
            n = 0
            while n < int(table.split):
                sql_head = 'ALTER TABLE `' + table.prefix + table.name + '_' + str(n) + '`\n'
                self.__write_file('./add.sql', sql_head + sql, 'w' if n == 0 else 'a')
                n += 1
                self.write_mode = 'a'
        else:
            sql_head = 'ALTER TABLE `' + table.prefix + table.name + '`\n'
            self.__write_file('./add.sql', sql_head + sql)

    def make_create_sql(self, table):

        if table.split_time:
            sql = 'CREATE TABLE IF NOT EXISTS `' + table.prefix + table.name + '_index` (\n'
            sql += '\t`id` bigint unsigned NOT NULL AUTO_INCREMENT,\n'
            sql += '\t`table_name` varchar(100) not null,\n'
            sql += '\t`time` datetime,\n'
            sql += '\t`verid` bigint,\n'
            sql += '\t`create_time` datetime,\n'
            sql += '\t`update_time` datetime,\n'
            sql += '\t`del` tinyint,\n'
            sql += '\tPRIMARY KEY(`id`),\n'
            sql += '\tINDEX index_table_name( `table_name` ),\n'
            sql += '\tINDEX index_time( `time` ),\n'
            sql += ') DEFAULT CHARSET=utf8;'
            sql += '\n'
            self.__write_file('./create.sql', sql)
        else:
            sql = ''
            # field
            for (field_name, field) in table.field_list.items():
                sql += '\t'
                sql += '`' + field.name + '`'
                sql += ' ' + field.type

                if field.size != '' and int(field.size) > 0:
                    sql += '(' + field.size + ')'

                if field.unsigned == 'true':
                    sql += ' unsigned'

                if field.charset != '':
                    if field.charset not in field.CHARSET_TYPE:
                        raise TypeError('charset error:' + field.charset)
                    else:
                        sql += ' CHARACTER SET ' + field.charset

                if field.value:
                    sql += ' DEFAULT \'' + field.param + '\''
                elif field.null == 'true':
                    sql += ' DEFAULT NULL'
                else:
                    sql += ' NOT NULL'

                if field.desc != '':
                    sql += ' COMMENT \'' + field.desc + '\''
                sql += ',\n'

            # default field
            sql += '\t`verid` bigint,\n'
            sql += '\t`create_time` datetime,\n'
            sql += '\t`update_time` datetime,\n'
            sql += '\t`del` tinyint,\n'

            # index
            if table.primary_key != '':
                sql += '\tPRIMARY KEY (' + self.deal_index(table.primary_key) + '),\n'

            for (index_name, index) in table.index_list.items():
                if index.unique == 'true':
                    sql += '\tUNIQUE KEY `index_' + index.name.replace(',', '_') + '`'
                else:
                    sql += '\tINDEX `index_' + index.name.replace(',', '_') + '`'

                sql += ' (' + self.deal_index(index.value, add_del=True) + '),\n'

            # default index
            sql += '\tUNIQUE KEY index_default_del(' + self.deal_index(table.primary_key, add_del=True) + ')'

            sql += '\n)'

            # table info
            if table.engine and table.engine in ('myisam', 'innodb'):
                sql += ' ENGINE=' + table.engine.upper()
            if table.charset:
                sql += ' DEFAULT CHARSET=' + table.charset
            if table.desc:
                sql += ' COMMENT=\'' + table.desc + '\''

            sql += ';\n\n'

            if table.split:
                n = 0
                while n < int(table.split):
                    sql_head = 'CREATE TABLE IF NOT EXISTS `' + table.prefix + table.name + '_' + str(n) + '` (\n'
                    self.__write_file('./create.sql', sql_head + sql, 'w' if n == 0 else 'a')
                    n += 1
            else:
                sql_head = 'CREATE TABLE IF NOT EXISTS `' + table.prefix + table.name + '` (\n'
                self.__write_file('./create.sql', sql_head + sql)

    def make_insert_sql(self, table):
        # field
        field_str = ''
        value_str = ''
        for (field_name, field) in table.field_list.items():
            field_str += '\t`' + field.name + '`,\n'
            if field.type in Field.INT_TYPE:
                value_str += '\t0,\n'
            else:
                value_str += '\t\'\',\n'

        # default
        field_str += '\t`verid`,\n'
        field_str += '\t`create_time`,\n'
        field_str += '\t`update_time`,\n'
        field_str += '\t`del`\n'
        value_str += '\t1,\n'
        value_str += '\t\'0000-00-00 00:00:00\',\n'
        value_str += '\t\'0000-00-00 00:00:00\',\n'
        value_str += '\t\'0000-00-00 00:00:00\'\n'

        sql = field_str
        sql += ') values (\n'
        sql += value_str
        sql += ');\n\n'

        if table.split:
            n = 0
            while n < int(table.split):
                sql_head = 'INSERT INTO `' + table.prefix + table.name + '_' + str(n) + '`(\n'
                self.__write_file('./insert.sql', sql_head + sql, 'w' if n == 0 else 'a')
                n += 1
        else:
            sql_head = 'INSERT INTO `' + table.prefix + table.name + '`(\n'
            self.__write_file('./insert.sql', sql_head + sql)

    def make_drop_sql(self, table):
        sql = ''
        # field
        for (field_name, field) in table.field_list.items():
            sql += '\tDROP COLUMN'
            sql += ' `' + field.name + '`'
            sql += ',\n'

        # index
        if table.primary_key != '':
            sql += '\tDROP PRIMARY KEY,\n'

        for (index_name, index) in table.index_list.items():
            if index.unique == 'true':
                sql += '\tDROP UNIQUE KEY `index_' + index.name.replace(',', '_') + '`'
            else:
                sql += '\tDROP INDEX `index_' + index.name.replace(',', '_') + '`'
            sql += ',\n'

        # default index
        sql += '\tDROP UNIQUE KEY index_default_del'

        sql += '\n;\n'

        if table.split:
            n = 0
            while n < int(table.split):
                sql_head = 'ALTER TABLE `' + table.prefix + table.name + '_' + str(n) + '`(\n'
                self.__write_file('./drop.sql', sql_head + sql, 'w' if n == 0 else 'a')
                n += 1
        else:
            sql_head = 'ALTER TABLE `' + table.prefix + table.name + '`\n'
            self.__write_file('./drop.sql', sql_head + sql)

    def deal_index(self, field, *, add_del=False):
        fd_arr = field.split(',')
        if add_del:
            fd_arr.append('del')

        r = map(self.__add_field_symbol, fd_arr)
        return ','.join(list(r))

    @staticmethod
    def __add_field_symbol(field):
        return '`' + field.strip() + '`'

    def __write_file(self, file_path, content, append=''):
        with open(file_path, append if append else self.write_mode, 1024, 'utf8') as fp:
            fp.write(content)
