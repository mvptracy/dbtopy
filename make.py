from dbtopy.field import Field


class Make(object):
    MYSQL_NAMESPACE = '\\Sooh\\DB\\Mysql'

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

                if field.param:
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

    def make_php_file(self, table, tables_namespace, tables_config, tables_db_type, tables_prefix):
        self.class_name = self.get_class_name(table.name)
        self.table_name = table.prefix + table.name

        head_str = '<?php\n'
        head_str += '//File Name: %s.php\n' % self.class_name
        head_str += '//This is is generated by py2db. Don\'t modify it!\n\n'

        namespace_str = 'namespace ' + table.namespace + ';\n\n'

        get_table_name_str = '\tpublic static function getTableName()\n'
        get_table_name_str += '\t{\n'
        get_table_name_str += '\t\treturn \'%s\';\n' % table.name
        get_table_name_str += '\t}\n'

        add_str = self.get_default_add_str(table)
        # print(head_str + namespace_str + get_table_name_str)

    def get_default_add_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * add a record\n'

        param_fields = []
        no_param_fields = []
        insert_field_str = ''
        insert_value_str = ''
        for i, (field_name, field) in enumerate(table.field_list.items()):
            if field.param or field.null == 'true':
                # 默认参数
                param_fields.append(field)
            else:
                # 无默认
                no_param_fields.append(field)

            if i == 0:
                insert_field_str += '\t' * 2 + '$sql .= \'`%s`\';\n' % field.name
                insert_value_str += '\t' * 2 + '$sql .= \':%s\';\n' % field.name
            else:
                insert_field_str += '\t' * 2 + '$sql .= \', `%s`\';\n' % field.name
                insert_value_str += '\t' * 2 + '$sql .= \', :%s\';\n' % field.name

        param_str = ''
        bind_str = ''
        for i, f in enumerate(no_param_fields):
            func_doc_comment += '\t * @param  $%s\n' % f.name
            bind_str += '\t' * 3 + '\':%s\' => $%s,\n' % (f.name, f.name)
            if i == 0:
                param_str += '\t' * 2 + ' ' * 2 + '$%s\n' % f.name
            else:
                param_str += '\t' * 2 + ', ' + '$%s\n' % f.name

        for f in param_fields:
            func_doc_comment += '\t * @param  $%s\n' % f.name
            bind_str += '\t' * 3 + '\':%s\' => $%s,\n' % (f.name, f.name)
            if param_str == '':
                param_str += '\t' * 2 + ' ' * 2 + '$%s\n' % f.name
            else:
                param_str += '\t' * 2 + ', ' + '$%s = %s\n' % (f.name, self.add_value_quotes(f))

        func_doc_comment += '\t */\n'

        final_str = func_doc_comment
        final_str += '\tpublic static function add(\n'
        final_str += param_str
        final_str += '\t)\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'\\\\Test\\\\Data\\\\%s::add\']))\n' % self.class_name
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'\\\\Test\\Data\\\\%s::add\'];\n' % self.class_name
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$bind = [\n'
        final_str += bind_str
        final_str += '\t' * 2 + '];\n'
        final_str += '\t' * 2 + '$datetime = new \DateTime;\n'
        final_str += '\t' * 2 + '$curDateTime = $datetime->format(\'Y-m-d H:i:s\');\n'
        final_str += '\t' * 2 + '$sql = \'insert into `%s`( \';\n' % self.table_name
        final_str += insert_field_str
        final_str += '\t' * 2 + '$sql .= \',  verid, create_time, update_time, del ) values( \';\n'
        final_str += insert_value_str
        final_str += '\t' * 2 + '$sql .= \',  \\\'1\\\', \\\'\' . $curDateTime . \'\\\', \\\'\' . $curDateTime . \'\\\', \\\'0\\\' )\';\n'
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + 'return $db->insert( $sql, $bind );\n'
        final_str += '\t}\n\n'
        print(final_str)


    @staticmethod
    def get_class_name(table_name):
        class_name = ''
        for word in table_name.split('_'):
            class_name += word.capitalize()

        return class_name + 'Data'

    @staticmethod
    def is_int(t):
        return t in Field.INT_TYPE

    @staticmethod
    def add_value_quotes(field):
        if field.type in Field.INT_TYPE:
            return field.param if field.param else 0
        else:
            return '\'%s\'' % field.param

    def add_field_quotes(self, field):
        if field.type in Field.INT_TYPE:
            return '$%s' % field.name
        elif field.type == 'datetime':
            return '\'\\\'\' . %s::timestamp2dbTime($%s) . \'\\\'\'' % (self.MYSQL_NAMESPACE, field.name)
        else:
            return '\'\\\'\' . $%s . \'\\\'\'' % field.name
