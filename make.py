from dbtopy.field import Field


class Make(object):
    MYSQL_NAMESPACE = '\\Sooh\\DB\\PdoMysql'

    def __init__(self, n, tables):
        if n == 1:
            self.write_mode = 'w'
        else:
            self.write_mode = 'a'
        self.tables = tables

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
            sql += '\tINDEX index_time( `time` )\n'
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

                if field.auto == 'true':
                    sql += ' AUTO_INCREMENT'

                if field.desc != '':
                    sql += ' COMMENT \'' + field.desc + '\''
                sql += ',\n'

            # default field
            # sql += '\t`verid` bigint,\n'
            # sql += '\t`create_time` datetime,\n'
            # sql += '\t`update_time` datetime,\n'
            # sql += '\t`del` tinyint,\n'

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
            elif field.type == 'datetime':
                value_str += '\t\'0000-00-00 00:00:00\',\n'
            else:
                value_str += '\t\'\',\n'

        # default
        # field_str += '\t`verid`,\n'
        # field_str += '\t`create_time`,\n'
        # field_str += '\t`update_time`,\n'
        # field_str += '\t`del`\n'
        # value_str += '\t1,\n'
        # value_str += '\t\'0000-00-00 00:00:00\',\n'
        # value_str += '\t\'0000-00-00 00:00:00\',\n'
        # value_str += '\t0,\n'

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

        r = map(self.add_field_symbol, fd_arr)
        return ','.join(list(r))

    @staticmethod
    # mysql字段2边添加`
    def add_field_symbol(field):
        return '`' + field.strip() + '`'

    # 写入文件
    def __write_file(self, file_path, content, append=''):
        with open(file_path, append if append else self.write_mode, 1024, 'utf8') as fp:
            fp.write(content)

    # 写php文件
    def make_php_file(self, table):
        self.class_name = self.get_class_name(table.name)
        self.table_name = table.prefix + table.name
        self.table_prefix = table.prefix

        head_str = '<?php\n'
        head_str += '//File Name: %s.php\n' % self.class_name
        head_str += '//This is is generated by py2db. Don\'t modify it!\n\n'
        head_str += 'namespace ' + table.namespace + ';\n\n'
        head_str += '/**\n'
        head_str += '* %s\n' % table.desc
        head_str += '*/\n'
        head_str += 'class %s extends \Sooh\Base\Obj\n{\n\n' % self.class_name

        bottom_str = '\n}\n'

        get_table_name_str = '\tpublic static function getTableName()\n'
        get_table_name_str += '\t{\n'
        get_table_name_str += '\t\treturn \'%s\';\n' % self.table_name
        get_table_name_str += '\t}\n'

        add_str = self.get_default_add_str(table)
        upd_str = self.get_default_upd_str(table)
        del_str = self.get_default_del_str(table)
        real_del_str = self.get_default_delreal_str(table)
        create_str = self.get_default_create_str(table)
        trans_str = self.get_default_trans_str(table)
        custom_del_str = self.get_custom_del_str(table)
        custom_upd_str = self.get_custom_upd_str(table)
        get_str = self.get_default_get_str(table)
        getall_str = self.get_default_getall_str(table)
        gettop_str = self.get_default_gettop_str(table)
        get_by_index_str = self.get_by_index_str(table)
        custom_select_str = self.get_custom_select_str(table)

        w_str = head_str + get_table_name_str + add_str + upd_str + del_str + real_del_str + create_str + trans_str + custom_del_str + custom_upd_str + get_str + getall_str + gettop_str + get_by_index_str + custom_select_str + bottom_str
        self.__write_file(self.class_name + '.php', w_str)
        self.__write_file(
            '/Users/tracy/work/project/meiyu/php/server/application/library/Test/Data/' + self.class_name + '.php',
            w_str)

    # function add
    def get_default_add_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * add a record\n'

        param_fields = []
        no_param_fields = []
        insert_field_str = ''
        insert_value_str = ''
        for i, (field_name, field) in enumerate(table.field_list.items()):
            if field.auto == 'true':
                continue
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
            bind_str += '\t' * 3 + '\':%s\' => %s,\n' % (f.name, self.get_bind_value(f))
            if i == 0:
                param_str += '\t' * 2 + ' ' * 2 + '$%s\n' % f.name
            else:
                param_str += '\t' * 2 + ', ' + '$%s\n' % f.name

        for f in param_fields:
            func_doc_comment += '\t * @param  $%s\n' % f.name
            bind_str += '\t' * 3 + '\':%s\' => %s,\n' % (f.name, self.get_bind_value(f))
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
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::add\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::add\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$bind = [\n'
        final_str += bind_str
        final_str += '\t' * 2 + '];\n'
        final_str += '\t' * 2 + '$datetime = new \DateTime;\n'
        final_str += '\t' * 2 + '$curDateTime = $datetime->format(\'Y-m-d H:i:s\');\n'
        final_str += '\t' * 2 + '$sql = \'insert into %s( \';\n' % self.add_field_symbol(self.table_name)
        final_str += insert_field_str
        final_str += '\t' * 2 + '$sql .= \',  `verid`, `create_time`, `update_time`, `del` ) values( \';\n'
        final_str += insert_value_str
        final_str += '\t' * 2 + '$sql .= \',  \\\'1\\\', \\\'\' . $curDateTime . \'\\\', \\\'\' . $curDateTime . \'\\\', \\\'0\\\' )\';\n'
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + 'return $db->insert( $sql, $bind );\n'
        final_str += '\t}\n\n'
        return final_str

    def get_default_upd_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * update a record\n'

        param_where_fields = []
        param_value_fields = []
        param_str = ''
        bind_str = ''
        upd_str = ''
        where_str = ''

        for field_name, field in table.field_list.items():
            if field_name in table.primary_key.split(','):
                param_where_fields.append(field)
            else:
                param_value_fields.append(field)

        for i, f in enumerate(param_where_fields):
            func_doc_comment += '\t * @param  $%s\n' % f.name
            bind_str += '\t' * 3 + '\':%s\' => %s,\n' % (f.name, self.get_bind_value(f))
            if i == 0:
                param_str += '\t' * 3 + '  $%s\n' % f.name
                where_str += '\t' * 2 + '$sql .= \'where %s = :%s' % (self.add_field_symbol(f.name), f.name)
            else:
                param_str += '\t' * 3 + ', $%s\n' % f.name
                where_str += ' AND %s = :%s' % (self.add_field_symbol(f.name), f.name)
        where_str += '\';\n'

        for i, f in enumerate(param_value_fields):
            func_doc_comment += '\t * @param  $%s\n' % f.name
            bind_str += '\t' * 3 + '\':%s\' => %s,\n' % (f.name, self.get_bind_value(f))
            param_str += '\t' * 3 + ', $%s\n' % f.name
            if i == 0:
                upd_str += '\t' * 2 + '$sql .= \'%s = :%s\' ;\n' % (self.add_field_symbol(f.name), f.name)
            else:
                upd_str += '\t' * 2 + '$sql .= \', %s = :%s\' ;\n' % (self.add_field_symbol(f.name), f.name)

        func_doc_comment += '\t */\n'

        final_str = func_doc_comment
        final_str += '\tpublic static function upd(\n'
        final_str += param_str
        final_str += '\t)\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::upd\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::upd\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$bind = [\n'
        final_str += bind_str
        final_str += '\t' * 2 + '];\n'
        final_str += '\t' * 2 + '$datetime = new \DateTime;\n'
        final_str += '\t' * 2 + '$curDateTime = $datetime->format(\'Y-m-d H:i:s\');\n'
        final_str += '\t' * 2 + '$sql = \'update %s set \';\n' % self.add_field_symbol(self.table_name)
        final_str += upd_str
        final_str += '\t' * 2 + '$sql .= \',\' . \'verid=verid+1, update_time=\\\'\' . $curDateTime . \'\\\'\';\n'
        final_str += where_str
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + 'return $db->update( $sql, $bind );\n'
        final_str += '\t}\n\n'

        return final_str

    def get_default_del_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * delete a record\n'

        bind_str = ''
        param_str = ''
        where_str = ''
        for f in table.primary_key.split(','):
            func_doc_comment += '\t * @param  $%s\n' % f
            bind_str = '\t' * 3 + '\':%s\' => $%s,\n' % (f, f)
            if param_str == '':
                param_str += '\t' * 2 + '$%s\n' % f
                where_str += '\t' * 2 + '$sql .= \'%s = :%s' % (self.add_field_symbol(f), f)
            else:
                param_str += '\t' * 2 + ', $%s\n' % f
                where_str += ' AND %s = :%s' % (self.add_field_symbol(f), f)

        where_str += '\';\n'
        func_doc_comment += '\t */\n'

        final_str = func_doc_comment
        final_str += '\tpublic static function del(\n'
        final_str += param_str
        final_str += '\t)\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::del\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::del\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$bind = [\n'
        final_str += bind_str
        final_str += '\t' * 2 + '];\n'
        final_str += '\t' * 2 + '$datetime = new \DateTime;\n'
        final_str += '\t' * 2 + '$curDateTime = $datetime->format(\'Y-m-d H:i:s\');\n'
        final_str += '\t' * 2 + '$sql = \'update %s set update_time=\\\'\' . $curDateTime . \'\\\', del=1 where \';\n' % self.add_field_symbol(
            self.table_name)
        final_str += where_str
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + 'return $db->update( $sql, $bind );\n'
        final_str += '\t}\n\n'

        return final_str

    def get_default_delreal_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * delete a record real\n'

        bind_str = ''
        param_str = ''
        where_str = ''
        for f in table.primary_key.split(','):
            func_doc_comment += '\t * @param  $%s\n' % f
            bind_str = '\t' * 3 + '\':%s\' => $%s,\n' % (f, f)
            if param_str == '':
                param_str += '\t' * 2 + '$%s\n' % f
                where_str += '\t' * 2 + '$sql .= \'%s = :%s' % (self.add_field_symbol(f), f)
            else:
                param_str += '\t' * 2 + ', $%s\n' % f
                where_str += ' AND %s = :%s' % (self.add_field_symbol(f), f)

        where_str += '\';\n'
        func_doc_comment += '\t */\n'

        final_str = func_doc_comment
        final_str += '\tpublic static function delReal(\n'
        final_str += param_str
        final_str += '\t)\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::delReal\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::delReal\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$bind = [\n'
        final_str += bind_str
        final_str += '\t' * 2 + '];\n'
        final_str += '\t' * 2 + '$datetime = new \DateTime;\n'
        final_str += '\t' * 2 + '$curDateTime = $datetime->format(\'Y-m-d H:i:s\');\n'
        final_str += '\t' * 2 + '$sql = \'delete from %s where \';\n' % self.add_field_symbol(self.table_name)
        final_str += where_str
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + 'return $db->delete( $sql, $bind );\n'
        final_str += '\t}\n\n'

        return final_str

    def get_default_create_str(self, table):
        final_str = '\tpublic static function create()\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::create\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::create\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'

        if table.split_time:
            sql = '\t' * 2 + '$sql = \'CREATE TABLE IF NOT EXISTS `' + self.table_name + '_index` ('
            sql += ' `id` bigint unsigned NOT NULL AUTO_INCREMENT,'
            sql += ' `table_name` varchar(100) not null,'
            sql += ' `time` datetime,'
            sql += ' `verid` bigint,'
            sql += ' `create_time` datetime,'
            sql += ' `update_time` datetime,'
            sql += ' `del` tinyint,'
            sql += ' PRIMARY KEY(`id`),'
            sql += ' INDEX index_table_name( `table_name` ),'
            sql += ' INDEX index_time( `time` )'
            sql += ') DEFAULT CHARSET=utf8;\''
            sql += '\n'

            final_str += sql
            final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
            final_str += '\t' * 2 + '$db->query( $sql );\n'
            final_str += '\t}\n\n'
            return final_str
        else:
            sql = ''
            # field
            for (field_name, field) in table.field_list.items():
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
                    sql += ' DEFAULT \\\'' + field.param + '\\\''
                elif field.null == 'true':
                    sql += ' DEFAULT NULL'
                else:
                    sql += ' NOT NULL'

                if field.desc != '':
                    sql += ' COMMENT \\\'' + field.desc + '\\\''
                sql += ','

            # default field
            sql += ' `verid` bigint,'
            sql += ' `create_time` datetime,'
            sql += ' `update_time` datetime,'
            sql += ' `del` tinyint,'

            # index
            if table.primary_key != '':
                sql += ' PRIMARY KEY (' + self.deal_index(table.primary_key) + '),'

            for (index_name, index) in table.index_list.items():
                if index.unique == 'true':
                    sql += ' UNIQUE KEY `index_' + index.name.replace(',', '_') + '`'
                else:
                    sql += ' INDEX `index_' + index.name.replace(',', '_') + '`'

                sql += ' (' + self.deal_index(index.value, add_del=True) + '),'

            # default index
            sql += ' UNIQUE KEY index_default_del(' + self.deal_index(table.primary_key, add_del=True) + ')'

            sql += ')'

            # table info
            if table.engine and table.engine in ('myisam', 'innodb'):
                sql += ' ENGINE=' + table.engine.upper()
            if table.charset:
                sql += ' DEFAULT CHARSET=' + table.charset
            if table.desc:
                sql += ' COMMENT=\\\'%s\\\'' % table.desc

            sql += '\';'

            if table.split:
                n = 0
                while n < int(table.split):
                    sql_head = '\t' * 2 + '$sql = \'CREATE TABLE IF NOT EXISTS `' + self.table_name + '_' + str(
                        n) + '` ('
                    final_str += sql_head + sql + '\n'
                    final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
                    final_str += '\t' * 2 + '$db->query( $sql );\n\n'
                    n += 1

            sql_head = '\t' * 2 + '$sql = \'CREATE TABLE IF NOT EXISTS `%s` (' % self.table_name
            final_str += sql_head + sql + '\n'
            final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
            final_str += '\t' * 2 + '$db->query( $sql );\n'
            final_str += '\t}\n\n'

        return final_str

    def get_default_trans_str(self, table):
        final_str = '\tpublic static function startTransaction()\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::startTransaction\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::startTransaction\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + 'return $db->startTransaction();\n'
        final_str += '\t}\n\n'

        final_str += '\tpublic static function endTransaction()\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::endTransaction\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::endTransaction\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + 'return $db->endTransaction();\n'
        final_str += '\t}\n\n'

        final_str += '\tpublic static function rollback()\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::rollback\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::rollback\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + 'return $db->rollback();\n'
        final_str += '\t}\n'

        return final_str

    def get_custom_upd_str(self, table):
        final_str = ''
        for upd_obj in table.update:
            func_doc_comment = ''
            param_str = ''
            bind_str = ''
            where_str = ''
            upd_str = ''
            self.tree_func_doc_comment = ''
            self.tree_param_str = ''

            # 更新的字段
            for (f_name, f) in upd_obj.field_list.items():
                self.tree_func_doc_comment += '\t * @param $%s\n' % f_name
                bind_str += '\t' * 3 + '\':%s\' => %s,\n' % (f_name, self.get_bind_value(table.field_list[f_name]))
                upd_str += '\t' * 2 + '$sql .= \', %s = :%s\';\n' % (self.add_field_symbol(f_name), f_name)
                if self.tree_param_str == '':
                    self.tree_param_str += '\t' * 2 + '$%s\n' % f_name
                else:
                    self.tree_param_str += '\t' * 2 + ', $%s\n' % f_name

            # where
            wlist = []
            for where in upd_obj.where_list:
                wlist.append(where.w)

            tree = self.deal_where_tree({'child': wlist}, table)

            func_doc_comment = self.tree_func_doc_comment
            param_str = self.tree_param_str
            where_str += tree[0]

            if upd_obj.lock == 'true':
                where_str += '\t' * 2 + '$bind[\':verid\'] = $oldVerId;\n'
                where_str += '\t' * 2 + '$sql .= \' AND verid=:verid\';\n'
                func_doc_comment += '\t * @param (=) $oldVerId\n'
                param_str += '\t' * 2 + ', $oldVerId\n'
            final_str += '\t/**\n'
            final_str += '\t * %s\n' % upd_obj.desc
            final_str += func_doc_comment
            final_str += '\t */\n'
            final_str += '\tpublic static function %s(\n' % upd_obj.name
            final_str += param_str
            final_str += '\t)\n'
            final_str += '\t{\n'
            final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::%s\']))\n' % (
                self.deal_namespace(table.namespace), self.class_name, upd_obj.name)
            final_str += '\t' * 2 + '{\n'
            final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::%s\'];\n' % (
                self.deal_namespace(table.namespace), self.class_name, upd_obj.name)
            final_str += '\t' * 2 + '}\n'
            final_str += '\t' * 2 + '$bind = [\n'
            final_str += bind_str
            final_str += '\t' * 2 + '];\n'
            final_str += '\t' * 2 + '$datetime = new \DateTime;\n'
            final_str += '\t' * 2 + '$curDateTime = $datetime->format(\'Y-m-d H:i:s\');\n'
            final_str += '\t' * 2 + '$sql = \'UPDATE %s SET \';\n' % self.add_field_symbol(self.table_name)
            final_str += '\t' * 2 + '$sql .= \'update_time = \\\'\' . $curDateTime . \'\\\'\';\n'
            final_str += '\t' * 2 + '$sql .= \', verid = verid + 1\';\n'
            final_str += upd_str
            final_str += '\t' * 2 + '$sql .= \' WHERE del=0\';\n'
            final_str += where_str
            final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
            final_str += '\t' * 2 + 'return $db->update( $sql, $bind );\n'
            final_str += '\t}\n\n'

        return final_str

    def get_custom_del_str(self, table):
        final_str = ''
        for del_obj in table.delete:
            func_doc_comment = ''
            param_str = ''
            bind_str = ''
            where_str = ''
            self.tree_func_doc_comment = ''
            self.tree_param_str = ''

            wlist = []
            for where in del_obj.where_list:
                wlist.append(where.w)

            tree = self.deal_where_tree({'child': wlist}, table)

            func_doc_comment = self.tree_func_doc_comment
            param_str = self.tree_param_str
            where_str = tree[0]

            final_str += '\t/**\n'
            final_str += '\t * %s\n' % del_obj.desc
            final_str += func_doc_comment
            final_str += '\t */\n'
            final_str += '\tpublic static function %s(\n' % del_obj.name
            final_str += param_str
            final_str += '\t)\n'
            final_str += '\t{\n'
            final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::%s\']))\n' % (
                self.deal_namespace(table.namespace), self.class_name, del_obj.name)
            final_str += '\t' * 2 + '{\n'
            final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::%s\'];\n' % (
                self.deal_namespace(table.namespace), self.class_name, del_obj.name)
            final_str += '\t' * 2 + '}\n'
            final_str += '\t' * 2 + '$datetime = new \DateTime;\n'
            final_str += '\t' * 2 + '$curDateTime = $datetime->format(\'Y-m-d H:i:s\');\n'

            if del_obj.real == 'false':
                final_str += '\t' * 2 + '$sql = \'UPDATE `%s` SET update_time = \\\'\' . $curDateTime . \'\\\', del=1\';\n' % self.table_name
            else:
                final_str += '\t' * 2 + '$sql = \'DELETE FROM `%s`\';\n' % self.table_name
            final_str += '\t' * 2 + '$sql .= \' WHERE del=0\';\n'
            final_str += where_str
            final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
            final_str += '\t' * 2 + 'return $db->%s( $sql, $bind );\n' % (
                'update' if del_obj.real == 'false' else 'delete')
            final_str += '\t}\n\n'

        return final_str

    def get_default_get_str(self, table):
        func_doc_comment = ''
        param_str = ''
        bind_str = ''
        where_str = ''

        func_doc_comment += '\t' + '/**\n'
        func_doc_comment += '\t' + ' * get a record.\n'
        for f_name in table.primary_key.replace(' ', '').split(','):
            field_obj = table.field_list[f_name]
            func_doc_comment += '\t' + ' * @param  $%s  %s\n' % (f_name, field_obj.desc)
            if param_str == '':
                param_str += '\t' * 2 + '$%s\n' % f_name
            else:
                param_str += '\t' * 2 + ', $%s\n' % f_name
            bind_str += '\t' * 3 + '\':%s\' => %s,\n' % (f_name, self.get_bind_value(field_obj))
            if where_str == '':
                where_str += ' . \'`%s` = :%s\'' % (f_name, f_name)
            else:
                where_str += ' . \' AND `%s` = :%s\'' % (f_name, f_name)

        func_doc_comment += '\t' + ' * @param $startRecordTimestamp\n'
        func_doc_comment += '\t' + ' * @param $endRecordTimestamp\n'
        func_doc_comment += '\t' + ' * @return a record or false if record not exist.\n'
        func_doc_comment += '\t' + ' */\n'

        final_str = ''
        final_str += func_doc_comment
        final_str += '\t' + 'public static function get(\n'
        final_str += param_str
        final_str += '\t)\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::get\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::get\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$bind = [\n'
        final_str += bind_str
        final_str += '\t' * 2 + '];\n'
        final_str += '\t' * 2 + '$sql = \'SELECT * FROM `%s` WHERE \'' % self.table_name
        final_str += where_str
        if table.logic_del == 'true':
            final_str += ' . \' AND del=0\';\n'
        else:
            final_str += ';\n'
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + '$rs = $db->fetchRow( $sql, $bind );\n'
        final_str += '\t' * 2 + 'if (!$rs || 0 == count($rs))\n\t\t{\n\t\t\treturn false;\n\t\t}\n'
        final_str += self.deal_result_to_timestamp(table, 'true')
        final_str += '\t' * 2 + 'return $rs;\n'
        final_str += '\t}\n\n'

        return final_str

    def get_default_getall_str(self, table):
        final_str = ''
        final_str += '\t' + '/**\n'
        final_str += '\t' + ' * get all records.\n'
        final_str += '\t' + ' * @return record array.\n'
        final_str += '\t' + ' */\n'
        final_str += '\t' + 'public static function getAll()\n'
        final_str += '\t' + '{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::getAll\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::getAll\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$sql = \'SELECT * FROM `%s`' % self.table_name
        if table.logic_del == 'true':
            final_str += ' WHERE del=0'
        final_str += '\';\n'
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + '$rs = $db->fetchAll( $sql );\n'
        final_str += self.deal_result_to_timestamp(table, 'false')
        final_str += '\t' * 2 + 'return $rs;\n'
        final_str += '\t' + '}\n'

        return final_str

    def get_default_gettop_str(self, table):
        final_str = ''
        final_str += '\t' + '/**\n'
        final_str += '\t' + ' * get top record.\n'
        final_str += '\t' + ' * @return record array.\n'
        final_str += '\t' + ' */\n'
        final_str += '\t' + 'public static function getTop($num = 1)\n'
        final_str += '\t' + '{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::getTop\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::getTop\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$sql = \'SELECT * FROM `%s`' % self.table_name
        if table.logic_del == 'true':
            final_str += ' WHERE del=0'
        final_str += ' LIMIT 0, \' . $num;\n'
        final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
        final_str += '\t' * 2 + '$rs = $db->fetchAll( $sql );\n'
        final_str += self.deal_result_to_timestamp(table, 'false')
        final_str += '\t' * 2 + 'return $rs;\n'
        final_str += '\t' + '}\n'

        return final_str

    def get_by_index_str(self, table):
        index_list = []
        final_str = ''
        for (index_name, index_obj) in table.index_list.items():
            t = {}
            t['name'] = index_obj.name
            t['value'] = index_obj.value
            t['unique'] = index_obj.unique
            index_list.append(t)
        # primary key
        t = {}
        t['name'] = table.primary_key
        t['value'] = table.primary_key
        t['unique'] = 'true'
        index_list.append(t)
        for index in index_list:
            param_str = ''
            func_doc_comment = ''
            bind_str = ''
            where_str = ''
            func_name = 'getBy'

            for f_name in index['value'].replace(' ','') .split(','):
                func_name += f_name.capitalize()
                func_doc_comment += '\t' + ' * @param  $%s  %s\n' % (f_name, table.field_list[f_name].desc)
                bind_str += '\t' * 3 + '\':%s\' => %s,\n' % (f_name, self.get_bind_value(table.field_list[f_name]))

                if param_str == '':
                    param_str += '\t' * 2 + '$%s\n' % f_name
                else:
                    param_str += '\t' * 2 + ', $%s\n' % f_name

                if where_str == '':
                    where_str += ' . \'`%s` = :%s\'' % (f_name, f_name)
                else:
                    where_str += ' . \' AND `%s` = :%s\'' % (f_name, f_name)

            final_str += '\t' + '/**\n'
            final_str += '\t' + ' * get data by %s\n' % index['name']
            final_str += func_doc_comment
            final_str += '\t' + ' */\n'
            final_str += '\t' + 'public static function %s(\n' % func_name
            final_str += param_str
            final_str += '\t)\n'
            final_str += '\t{\n'
            final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::%s\']))\n' % (
                self.deal_namespace(table.namespace), self.class_name, func_name)
            final_str += '\t' * 2 + '{\n'
            final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::%s\'];\n' % (
                self.deal_namespace(table.namespace), self.class_name, func_name)
            final_str += '\t' * 2 + '}\n'
            final_str += '\t' * 2 + '$bind = [\n'
            final_str += bind_str
            final_str += '\t' * 2 + '];\n'
            final_str += '\t' * 2 + '$sql = \'SELECT * FROM `%s` WHERE \'' % self.table_name
            final_str += where_str
            if table.logic_del == 'true':
                final_str += ' . \' AND del=0\';\n'
            else:
                final_str += ';\n'
            final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
            if index['unique'] == 'true':
                final_str += '\t' * 2 + '$rs = $db->fetchRow( $sql, $bind );\n'
            else:
                final_str += '\t' * 2 + '$rs = $db->fetchAll( $sql, $bind );\n'
            final_str += '\t' * 2 + 'if (!$rs || 0 == count($rs))\n\t\t{\n\t\t\treturn false;\n\t\t}\n'
            final_str += self.deal_result_to_timestamp(table, index['unique'])
            final_str += '\t' * 2 + 'return $rs;\n'
            final_str += '\t' + '}\n\n'

        return final_str

    def get_custom_select_str(self, table):
        final_str = ''
        self.table_prefix = table.prefix

        for sel_obj in table.select:
            func_doc_comment = ''
            param_str = ''
            bind_str = ''
            where_str = ''
            fields_str = ''
            join_str = ''
            self.tree_func_doc_comment = ''
            self.tree_param_str = ''

            if sel_obj.join_list:
                has_join = True
            else:
                has_join = False

            datetime_fields = []
            # 查询的字段
            for f in sel_obj.field_list:
                f_name = f['name']

                if f_name == '*':
                    break

                if f_name in self.tables.table[f.get('table', table.name)].field_list:
                    f_obj = self.tables.table[f.get('table', table.name)].field_list[f_name]
                    if f_obj.type == 'datetime' and f_name not in ('create_time', 'update_time') and f.get('origin', 'false') == 'false':
                        datetime_fields.append(f_name)

                if f.get('origin', 'false') == 'false':
                    f_name = self.add_field_symbol(f_name)

                if has_join:
                    f_name = '`%s`.' % (f.get('table_prefix',table.prefix ) + f.get('table',table.name)) + f_name

                if f.get('unique', 'false') == 'true':
                    f_name = 'DISTINCT ' + f_name

                if f.get('func') and f['func'] in ('sum','count','max','min'):
                    f_name = '%s(%s)' % (f['func'].upper(), f_name)

                if f.get('alias'):
                    f_name += ' AS %s' % f['alias']
                fields_str += f_name + ', '

            # 无field查*
            if fields_str == '':
                fields_str = '*'
                for (k, f) in table.field_list.items():
                    if f.type == 'datetime':
                        datetime_fields.append(f.name)
            else:
                fields_str = fields_str[:-2]

            # join
            join_tables = []
            for j in sel_obj.join_list:
                join_str += self.deal_join(j.join)
                join_tables.append(j.join.get('table_prefix', self.table_prefix) + j.join['table'])
                if fields_str == '*':
                    for (k,f) in self.tables.table[j.join['table']].field_list.items():
                        if f.type == 'datetime':
                            datetime_fields.append(f.name)


            # where
            if sel_obj.logic_del == 'true' and len(join_tables) == 0:
                where_str += '\t' * 2 + '$sql .= \' WHERE del=0\';\n'
            elif sel_obj.logic_del == 'true' and len(join_tables) > 0:
                where_str += '\t' * 2 + '$sql .= \' WHERE `%s`.del=0' % self.table_name
                for tb_name in join_tables:
                    where_str += ' AND `%s`.del=0' % tb_name
                where_str += '\';\n'
            else:
                where_str += '\t' * 2 + '$sql .= \' WHERE 1\';\n'

            wlist = []
            for where in sel_obj.where_list:
                wlist.append(where.w)

            tree = self.deal_where_tree({'child': wlist}, table)
            func_doc_comment = self.tree_func_doc_comment
            param_str = self.tree_param_str
            where_str += tree[0]

            final_str += '\t/**\n'
            final_str += '\t * %s\n' % sel_obj.desc
            final_str += func_doc_comment
            final_str += '\t */\n'
            final_str += '\tpublic static function %s(\n' % sel_obj.name
            final_str += param_str
            final_str += '\t)\n'
            final_str += '\t{\n'
            final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::%s\']))\n' % (
                self.deal_namespace(table.namespace), self.class_name, sel_obj.name)
            final_str += '\t' * 2 + '{\n'
            final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::%s\'];\n' % (
                self.deal_namespace(table.namespace), self.class_name, sel_obj.name)
            final_str += '\t' * 2 + '}\n'
            final_str += '\t' * 2 + '$bind = [];\n'
            final_str += '\t' * 2 + '$sql = \'SELECT %s FROM `%s`\';\n' % (fields_str, self.table_name)
            final_str += join_str
            final_str += where_str
            final_str += '\t' * 2 + '$db = %s::getInstance( \'%s\' );\n' % (self.MYSQL_NAMESPACE, table.config)
            if sel_obj.single == 'true':
                final_str += '\t' * 2 + '$rs = $db->fetchRow( $sql, $bind );\n'
            else:
                final_str += '\t' * 2 + '$rs = $db->fetchAll( $sql, $bind );\n'
            if final_str == '*':
                final_str += deal_result_to_timestamp(table,)
            final_str += self.deal_result_to_timestamp_fields(datetime_fields, sel_obj.single)
            final_str += '\t' * 2 + 'return $rs;\n'
            final_str += '\t' + '}\n\n'

        return final_str

    @staticmethod
    # 获取类名
    def get_class_name(table_name):
        class_name = ''
        for word in table_name.split('_'):
            class_name += word.capitalize()

        return class_name + 'Data'

    @staticmethod
    def is_int(t):
        return t in Field.INT_TYPE

    @staticmethod
    # 非数字value自动加引号
    def add_value_quotes(field):
        if field.type in Field.INT_TYPE:
            return field.param if field.param else 0
        else:
            return '\'%s\'' % field.param

    def get_bind_value(self, field, variable_name=None, value=None):
        if value != None:
            if self.is_int(field.type):
                return value
            else:
                return '\'' + value + '\''

        if variable_name != None:
            f_name = variable_name
        else:
            f_name = field.name

        if field.type == 'datetime':
            return '%s::timestamp2dbTime($%s)' % (self.MYSQL_NAMESPACE, f_name)
        else:
            return '$' + f_name

    def deal_namespace(self, namespace):
        return '\\\\' + '\\\\'.join(namespace.split('\\'))

    @staticmethod
    def replace_spec_string(s):
        if s == 'lt':
            return '<'
        elif s == 'lteq':
            return '<='
        elif s == 'ltgt':
            return '<>'
        else:
            return s

    def get_field_table(self, w, table_name):
        field_table = ''
        if 'table_prefix' in w:
            field_table += w['table_prefix']
        else:
            field_table += self.table_prefix

        if 'table' in w:
            field_table += w['table']
        else:
            field_table += table_name

        if not field_table:
            field_table = self.table_name

        return field_table

    def deal_where_tree(self, where_list, table, suffix=''):
        tb_name = table.name
        where_str = ''
        where_suffix = ''
        where_name = ''
        if 'type' in where_list:
            type = where_list['type'].upper()
            if 'name' in where_list:
                where_suffix = '_' + where_list['name']
                where_name = where_list['name']
            else:
                where_suffix = '_'
        else:
            type = 'AND'

        if suffix:
            where_suffix += suffix

        if where_suffix:
            sql_name = 'sql_' + tb_name + where_suffix + '_'
        else:
            sql_name = 'sql'

        variable_name_list = []
        for row in where_list['child']:

            if 'child' not in row:
                field_table_prefix = row.get('table_prefix', table.prefix)
                field_table_name = row.get('table', table.name)
                # 处理参数名
                f_name = row['name']
                if 'suffix' in row:
                    variable_name = field_table_name + '_' + f_name + '_' + row['suffix'] + where_suffix
                else:
                    variable_name = field_table_name + '_' + f_name + '_' + 'cond' + where_suffix

                if 'comp' in row:
                    if 'in' == row['comp']:
                        variable_name += '_include'
                    elif 'not in' == row['comp']:
                        variable_name += '_exclude'
                    comp = self.replace_spec_string(row['comp']).upper()
                else:
                    comp = '='

                if 'value' in row:
                    # 有默认值
                    where_str += '\t' * 2 + 'if ( true )\n'
                    where_str += '\t' * 2 + '{\n'
                    if comp in ('IN', 'NOT IN'):
                        where_str += '\t' * 3 + '$%s = \'%s\';\n' % (variable_name, row['value'])
                        where_str += '\t' * 3 + '$%s = trim($%s, \'()\\s\');\n' % (variable_name, variable_name)
                        where_str += '\t' * 3 + '$bind[\':%s\'] = explode(\',\', $%s);\n' % (
                        variable_name, variable_name)
                    else:
                        where_str += '\t' * 3 + '$bind[\':%s\'] = %s;\n' % (
                            variable_name,
                            self.get_bind_value(self.tables.table[field_table_name].field_list[f_name], variable_name, row['value']))
                else:
                    self.tree_func_doc_comment += '\t * @param (%s) $%s\n' % (comp, variable_name)
                    if self.tree_param_str:
                        self.tree_param_str += '\t' * 2 + ', $%s\n' % variable_name
                    else:
                        self.tree_param_str += '\t' * 2 + '$%s\n' % variable_name
                    where_str += '\t' * 2 + 'if ( false !== $%s )\n' % variable_name
                    where_str += '\t' * 2 + '{\n'
                    if comp in ('IN', 'NOT IN'):
                        where_str += '\t' * 3 + 'if (!is_array($%s))\n' % variable_name
                        where_str += '\t' * 3 + '{\n'
                        where_str += '\t' * 4 + '$%s = explode(\',\', $%s);\n' % (variable_name, variable_name)
                        where_str += '\t' * 3 + '}\n'
                        where_str += '\t' * 3 + '$bind[\':%s\'] = $%s;\n' % (
                            variable_name, variable_name)
                    else:
                        where_str += '\t' * 3 + '$bind[\':%s\'] = %s;\n' % (
                            variable_name,
                            self.get_bind_value(self.tables.table[field_table_name].field_list[f_name], variable_name))

                if comp in ('IN', 'NOT IN'):
                    where_str += '\t' * 3 + '$sql_%s = \'(`%s`.`%s` %s (:%s))\';\n' % (
                        variable_name, self.get_field_table(row, table.name), f_name, comp, variable_name)
                else:
                    where_str += '\t' * 3 + '$sql_%s = \'(`%s`.`%s` %s :%s)\';\n' % (
                        variable_name, self.get_field_table(row, table.name), f_name, comp, variable_name)
                where_str += '\t' * 2 + '}\n'

                variable_name_list.append(variable_name)
            else:
                # 嵌套
                tree = self.deal_where_tree(row, table, where_suffix)
                where_str += tree[0]
                variable_name_list.append(tree[1])

        # type拼接
        where_str += '\n\t\t// table:' + tb_name + ', name:' + where_name + ' type:' + type + '\n'
        if sql_name != 'sql':
            where_str += '\t' * 2 + '$%s = \'\';\n' % sql_name
            where_str += '\t' * 2 + '$first = true;\n'

        for v in variable_name_list:
            if sql_name == 'sql':
                where_str += '\t' * 2 + 'if (false !== $sql_%s)\n' % v
                where_str += '\t' * 2 + '{\n'
                where_str += '\t' * 3 + '$sql .= \' AND \' . $sql_%s;\n' % v
                where_str += '\t' * 2 + '}\n'
            else:
                where_str += '\t' * 2 + 'if(strlen($sql_%s) > 0)\n' % v
                where_str += '\t' * 2 + '{\n'
                where_str += '\t' * 3 + 'if(!$first)\n'
                where_str += '\t' * 3 + '{\n'
                where_str += '\t' * 4 + '$%s .= \' %s \';\n' % (sql_name, type)
                where_str += '\t' * 3 + '}\n'
                where_str += '\t' * 3 + '$%s .= $sql_%s;\n' % (sql_name, v)
                where_str += '\t' * 3 + '$first = false;\n'
                where_str += '\t' * 2 + '}\n'
                where_str += '\t' * 2 + 'if(!$first)\n'

        if sql_name != 'sql':
            where_str += '\t' * 2 + '{\n'
            where_str += '\t' * 3 + '$%s = \'(\' . $%s . \')\';\n' % (sql_name, sql_name)
            where_str += '\t' * 2 + '}\n'

        return [where_str, sql_name[4:]]

    def deal_result_to_timestamp(self, table, unique):
        final_str = ''
        if unique == 'true':
            for (f_name, field_obj) in table.field_list.items():
                if field_obj.type == 'datetime':
                    final_str += '\t' * 2 + '$rs[\'%s\'] = %s::dbTime2Timestamp($rs[\'%s\']);\n' % (
                        field_obj.name, self.MYSQL_NAMESPACE, field_obj.name)

            # final_str += '\t' * 2 + '$rs[\'create_time\'] = %s::dbTime2Timestamp($rs[\'create_time\']);\n' % self.MYSQL_NAMESPACE
            # final_str += '\t' * 2 + '$rs[\'update_time\'] = %s::dbTime2Timestamp($rs[\'update_time\']);\n' % self.MYSQL_NAMESPACE
        else:
            final_str += '\t' * 2 + 'if ($rs)\n'
            final_str += '\t' * 2 + '{\n'
            final_str += '\t' * 3 + 'foreach($rs as $k => $v)\n'
            final_str += '\t' * 3 + '{\n'

            for (f_name, field_obj) in table.field_list.items():
                if field_obj.type == 'datetime':
                    final_str += '\t' * 4 + '$v[\'%s\'] = %s::dbTime2Timestamp($v[\'%s\']);\n' % (
                        field_obj.name, self.MYSQL_NAMESPACE, field_obj.name)

            # final_str += '\t' * 4 + '$v[\'create_time\'] = %s::dbTime2Timestamp($v[\'create_time\']);\n' % self.MYSQL_NAMESPACE
            # final_str += '\t' * 4 + '$v[\'update_time\'] = %s::dbTime2Timestamp($v[\'update_time\']);\n' % self.MYSQL_NAMESPACE
            final_str += '\t' * 4 + '$rs[$k] = $v;\n'
            final_str += '\t' * 3 + '}\n'
            final_str += '\t' * 2 + '}\n'

        return final_str

    def deal_result_to_timestamp_fields(self, fields, unique):
        fields = list(set(fields))
        final_str = ''
        if unique == 'true':
            for f in fields:
                final_str += '\t' * 2 + '$rs[\'%s\'] = %s::dbTime2Timestamp($rs[\'%s\']);\n' % (
                    f, self.MYSQL_NAMESPACE, f)
        else:
            final_str += '\t' * 2 + 'if ($rs)\n'
            final_str += '\t' * 2 + '{\n'
            final_str += '\t' * 3 + 'foreach($rs as $k => $v)\n'
            final_str += '\t' * 3 + '{\n'
            for f in fields:
                final_str += '\t' * 4 + '$v[\'%s\'] = %s::dbTime2Timestamp($v[\'%s\']);\n' % (
                    f, self.MYSQL_NAMESPACE, f)
            final_str += '\t' * 4 + '$rs[$k] = $v;\n'
            final_str += '\t' * 3 + '}\n'
            final_str += '\t' * 2 + '}\n'

        return final_str

    def deal_join(self, join):
        join_str = '\t' * 2 + '$sql .= \' %s JOIN `%s` on \';\n' % (join['type'].upper(), join.get('table_prefix', self.table_prefix) + join['table'])
        join_str += self.deal_join_cond(join)
        return join_str

    def deal_join_cond(self, cond, type='and'):
        cond_str = ''
        i = 0
        if 'cond' in cond:
            cond = cond['cond']
        for c in cond:
            if 'child' in c:
                if i > 0:
                    cond_str += '\t' * 2 + '$sql .= \' %s \';\n' % type.upper()
                cond_str += '\t' * 2 + '$sql .= \'(\';\n';
                cond_str += self.deal_join_cond(c['child'], c['type'])
                cond_str += '\t' * 2 + '$sql .= \')\';\n';
                i += 1
            else:
                if i > 0:
                    cond_str += '\t' * 2 + '$sql .= \' %s \';\n' % type.upper()
                cond_str += '\t' * 2 + '$sql .= \'(`%s`.`%s` = `%s`.`%s`)\';\n' % (
                c.get('table_prefix1', self.table_prefix) + c['table1'], c['field1'],
                c.get('table_prefix2', self.table_prefix) + c['table2'], c['field2'])
                i += 1

        return cond_str