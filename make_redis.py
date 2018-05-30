from db_py.field import Field


class MakeRedis(object):

    def __init__(self, tables):
        self.tables = tables


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

        add_str = self.get_default_add_str(table)
        upd_str = self.get_default_upd_str(table)
        get_str = self.get_default_get_str(table)
        get_index_str = self.get_default_get_index_str(table)
        getall_str = self.get_default_getall_str(table)
        del_str = self.get_default_del_str(table)
        real_del_str = self.get_default_delreal_str(table)
        # gettop_str = self.get_default_gettop_str(table)

        w_str = head_str + add_str + upd_str + get_str + get_index_str + getall_str + del_str + real_del_str + bottom_str
                # + upd_str + del_str + real_del_str + create_str  + getall_str + gettop_str \

        self.__write_file(self.class_name + '.php', w_str, 'w')

    @staticmethod
    # 获取类名
    def get_class_name(table_name):
        class_name = ''
        for word in table_name.split('_'):
            class_name += word.capitalize()
        return class_name + 'Data'

    @staticmethod
    # 非数字value自动加引号
    def add_value_quotes(field):
        if field.type in Field.INT_TYPE:
            return field.param if field.param else 0
        else:
            return '\'%s\'' % field.param

    @staticmethod
    def deal_namespace(namespace):
        return '\\\\' + '\\\\'.join(namespace.split('\\'))

    # 写入文件
    def __write_file(self, file_path, content, append=''):
        with open(file_path, 'w', 1024, 'utf8') as fp:
            fp.write(content)

    def get_default_add_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * add a record\n'

        param_fields = []
        no_param_fields = []
        for i, (field_name, field) in enumerate(table.field_list.items()):
            if field_name in table.DEFAULT_FIELDS:
                continue
            if field.param or field.null == 'true':
                # 默认参数
                param_fields.append(field)
            else:
                # 无默认
                no_param_fields.append(field)

        param_str = ''
        values_str = ''
        for i, f in enumerate(no_param_fields):
            func_doc_comment += '\t * @param  $%s\t%s\n' % (f.name, f.desc)
            values_str += '\t' * 3 + '\'%s\' => $%s,\n' % (f.name, f.name)
            if i == 0:
                param_str += '\t' * 2 + ' ' * 2 + '$%s\n' % f.name
            else:
                param_str += '\t' * 2 + ', ' + '$%s\n' % f.name

        for f in param_fields:
            func_doc_comment += '\t * @param  $%s\t%s\n' % (f.name, f.desc)
            values_str += '\t' * 3 + '\'%s\' => $%s,\n' % (f.name, f.name)
            if param_str == '':
                param_str += '\t' * 2 + ' ' * 2 + '$%s\n' % f.name
            else:
                param_str += '\t' * 2 + ', ' + '$%s = %s\n' % (f.name, self.add_value_quotes(f))

        func_doc_comment += '\t */\n'

        values_str += '\t' * 3 + '\'verid\' => 1,\n'
        values_str += '\t' * 3 + '\'create_time\' => $curDateTime,\n'
        values_str += '\t' * 3 + '\'update_time\' => $curDateTime,\n'
        values_str += '\t' * 3 + '\'del\' => 0,\n'

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
        final_str += '\t' * 2 + '$key = \'\';\n'
        final_str += '\t' * 2 + '$key .= $%s;\n' % table.primary_key
        final_str += '\t' * 2 + '$curDateTime = \\Sooh\\Base\\Utils::getTime();\n'
        final_str += '\t' * 2 + '$values = [\n'
        final_str += values_str
        final_str += '\t' * 2 + '];\n'
        final_str += '\t' * 2 + '$db = \Sooh\DB\KRedis::getInstance(\'%s\');\n' % table.config
        final_str += '\t' * 2 + 'if (false === $db->set(\n'
        final_str += '\t' * 4 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 4 + '$key,\n'
        final_str += '\t' * 4 + 'json_encode($values)\n'
        final_str += '\t' * 3 + '))\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '\n'
        final_str += '\t' * 2 + '$tableIndex = \'%s_index_%s_del\';\n' % (self.table_name, table.primary_key)
        final_str += '\t' * 2 + '$keyIndex = $%s;\n' % table.primary_key
        final_str += '\t' * 2 + '$rs = $db->get(\n'
        final_str += '\t' * 3 + '$tableIndex,\n'
        final_str += '\t' * 3 + '$keyIndex\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if (!$rs)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + '$rs = [$key];\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'else\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + '$rs = json_decode(\n'
        final_str += '\t' * 4 + '$rs,\n'
        final_str += '\t' * 4 + 'true\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 3 + '$tmp = $rs;\n'
        final_str += '\t' * 3 + '$rs = [];\n'
        final_str += '\t' * 3 + '$isOld = false;\n'
        final_str += '\t' * 3 + 'foreach ($tmp as $k)\n'
        final_str += '\t' * 3 + '{\n'
        final_str += '\t' * 4 + 'if ($key === $k)\n'
        final_str += '\t' * 4 + '{\n'
        final_str += '\t' * 5 + '$isOld = true;\n'
        final_str += '\t' * 4 + '}\n'
        final_str += '\t' * 4 + 'if ($db->exists(\n'
        final_str += '\t' * 5 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 5 + '$k\n'
        final_str += '\t' * 4 + '))\n'
        final_str += '\t' * 4 + '{\n'
        final_str += '\t' * 5 + '$rs[] = $k;\n'
        final_str += '\t' * 4 + '}\n'
        final_str += '\t' * 3 + '}\n'
        final_str += '\t' * 3 + 'if (!$isOld)\n'
        final_str += '\t' * 3 + '{\n'
        final_str += '\t' * 4 + '$rs[] = $key;\n'
        final_str += '\t' * 3 + '}\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'if (false === $db->set(\n'
        final_str += '\t' * 4 + '$tableIndex,\n'
        final_str += '\t' * 4 + '$keyIndex,\n'
        final_str += '\t' * 4 + 'json_encode($rs)\n'
        final_str += '\t' * 3 + '))\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'return true;\n'
        final_str += '\t' * 1 + '}\n'

        return final_str

    def get_default_upd_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * update a record\n'

        param_where_fields = []
        param_value_fields = []
        param_str = ''

        values_str = '\t' * 3 + '\'id\' => $key,\n'

        for field_name, field in table.field_list.items():
            if field_name in table.primary_key.split(','):
                param_where_fields.append(field)
                values_str += '\t' * 3 + '\'%s\' => $rs[\'%s\'],\n' % (field.name, field.name)
            else:
                param_value_fields.append(field)
                values_str += '\t' * 3 + '\'%s\' => $%s,\n' % (field.name, field.name)

        for i, f in enumerate(param_where_fields):
            func_doc_comment += '\t * @param  $%s\t%s\n' % (f.name, f.desc)
            if i == 0:
                param_str += '\t' * 2 + '  $%s\n' % f.name
            else:
                param_str += '\t' * 2 + ', $%s\n' % f.name

        for i, f in enumerate(param_value_fields):
            func_doc_comment += '\t * @param  $%s\t%s\n' % (f.name, f.desc)
            param_str += '\t' * 2 + ', $%s\n' % f.name

        func_doc_comment += '\t */\n'

        values_str += '\t' * 3 + '\'verid\' => $rs[\'verid\'] + 1,\n'
        values_str += '\t' * 3 + '\'update_time\' => $curDateTime,\n'
        values_str += '\t' * 3 + '\'del\' => 0,\n'

        final_str = func_doc_comment
        final_str += '\tpublic static function upd(\n'
        final_str += param_str
        final_str += '\t)\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::add\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::add\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$key = \'\';\n'
        final_str += '\t' * 2 + '$key .= $%s;\n' % table.primary_key
        final_str += '\t' * 2 + '$curDateTime = \\Sooh\\Base\\Utils::getTime();\n'
        final_str += '\t' * 2 + '$db = \Sooh\DB\KRedis::getInstance(\'%s\');\n' % table.config
        final_str += '\t' * 2 + '$rs = $db->get(\n'
        final_str += '\t' * 3 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 3 + '$key\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if ($rs)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + '$rs = json_decode(\n'
        final_str += '\t' * 4 + '$rs,\n'
        final_str += '\t' * 4 + 'true\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'else\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + '$rs = [\'verid\' => 0];\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$values = [\n'
        final_str += values_str
        final_str += '\t' * 2 + '];\n'
        final_str += '\t' * 2 + 'if (false === $db->set(\n'
        final_str += '\t' * 4 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 4 + '$key,\n'
        final_str += '\t' * 4 + 'json_encode($values)\n'
        final_str += '\t' * 3 + '))\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '\n'

        final_str += '\t' * 2 + '$tableIndex = \'%s_index_%s_del\';\n' % (self.table_name, table.primary_key)
        final_str += '\t' * 2 + '$keyIndex = $%s;\n' % table.primary_key
        final_str += '\t' * 2 + '$rs = $db->get(\n'
        final_str += '\t' * 3 + '$tableIndex,\n'
        final_str += '\t' * 3 + '$keyIndex\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if (!$rs)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + '$rs = [$key];\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'else\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + '$rs = json_decode(\n'
        final_str += '\t' * 4 + '$rs,\n'
        final_str += '\t' * 4 + 'true\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 3 + '$tmp = $rs;\n'
        final_str += '\t' * 3 + '$rs = [];\n'
        final_str += '\t' * 3 + '$isOld = false;\n'
        final_str += '\t' * 3 + 'foreach ($tmp as $k)\n'
        final_str += '\t' * 3 + '{\n'
        final_str += '\t' * 4 + 'if ($key === $k)\n'
        final_str += '\t' * 4 + '{\n'
        final_str += '\t' * 5 + '$isOld = true;\n'
        final_str += '\t' * 4 + '}\n'
        final_str += '\t' * 4 + 'if ($db->exists(\n'
        final_str += '\t' * 5 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 5 + '$k\n'
        final_str += '\t' * 4 + '))\n'
        final_str += '\t' * 4 + '{\n'
        final_str += '\t' * 5 + '$rs[] = $k;\n'
        final_str += '\t' * 4 + '}\n'
        final_str += '\t' * 3 + '}\n'
        final_str += '\t' * 3 + 'if (!$isOld)\n'
        final_str += '\t' * 3 + '{\n'
        final_str += '\t' * 4 + '$rs[] = $key;\n'
        final_str += '\t' * 3 + '}\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'if (false === $db->set(\n'
        final_str += '\t' * 4 + '$tableIndex,\n'
        final_str += '\t' * 4 + '$keyIndex,\n'
        final_str += '\t' * 4 + 'json_encode($rs)\n'
        final_str += '\t' * 3 + '))\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'return true;\n'
        final_str += '\t' * 1 + '}\n'

        return final_str

    def get_default_get_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * get a record.\n'
        param_str = ''

        for f_name in table.primary_key.split(','):
            field_obj = table.field_list[f_name]
            func_doc_comment += '\t' + ' * @param  $%s\t%s\n' % (f_name, field_obj.desc)
            if param_str == '':
                param_str += '\t' * 2 + '$%s\n' % f_name
            else:
                param_str += '\t' * 2 + ', $%s\n' % f_name

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
        final_str += '\t' * 2 + '$key = \'\';\n'
        final_str += '\t' * 2 + '$key .= $%s;\n' % table.primary_key
        final_str += '\t' * 2 + '$db = \Sooh\DB\KRedis::getInstance(\'%s\');\n' % table.config
        final_str += '\t' * 2 + '$rs = $db->get(\n'
        final_str += '\t' * 3 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 3 + '$key\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if (!$rs)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$rs = json_decode(\n'
        final_str += '\t' * 3 + '$rs,\n'
        final_str += '\t' * 3 + 'true\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if (0 != $rs[\'del\'])\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '\n'
        final_str += '\t' * 2 + 'return $rs;\n'
        final_str += '\t' * 1 + '}\n'

        return final_str

    def get_default_get_index_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * get data by %s,del\n' % table.primary_key
        param_str = ''

        for f_name in table.primary_key.split(','):
            field_obj = table.field_list[f_name]
            func_doc_comment += '\t' + ' * @param  $%s\t%s\n' % (f_name, field_obj.desc)
            if param_str == '':
                param_str += '\t' * 2 + '$%s\n' % f_name
            else:
                param_str += '\t' * 2 + ', $%s\n' % f_name
        param_str += '\t' * 2 + ', $del\n'

        func_doc_comment += '\t' + ' * @return record array\n'
        func_doc_comment += '\t' + ' */\n'

        func_name = 'getBy%sDel' % table.primary_key.capitalize()

        final_str = ''
        final_str += func_doc_comment
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
        final_str += '\t' * 2 + '$keyIndex = $%s;\n' % table.primary_key
        final_str += '\t' * 2 + '$tableIndex = \'%s_index_%s_del\';\n' % (self.table_name, table.primary_key)
        final_str += '\t' * 2 + '$db = \Sooh\DB\KRedis::getInstance(\'%s\');\n' % table.config
        final_str += '\t' * 2 + '$keys = $db->get(\n'
        final_str += '\t' * 3 + '$tableIndex,\n'
        final_str += '\t' * 3 + '$keyIndex\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if (!$keys)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$keys = json_decode(\n'
        final_str += '\t' * 3 + '$keys,\n'
        final_str += '\t' * 3 + 'true\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + '$rs = $db->get(\n'
        final_str += '\t' * 3 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 3 + '$keys\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if (!$rs)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$ret = [];\n'
        final_str += '\t' * 2 + 'foreach ($rs as $v)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + '$ret[] = json_decode(\n'
        final_str += '\t' * 4 + '$v,\n'
        final_str += '\t' * 4 + 'true\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '\n'
        final_str += '\t' * 2 + 'return $ret;\n'
        final_str += '\t' * 1 + '}\n'

        return final_str

    def get_default_getall_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * get all records\n'
        func_doc_comment += '\t' + ' * @return record array\n'
        func_doc_comment += '\t' + ' */\n'

        final_str = ''
        final_str += func_doc_comment
        final_str += '\t' + 'public static function getAll()\n'
        final_str += '\t{\n'
        final_str += '\t' * 2 + 'if (isset($GLOBALS[\'db_test\']) && isset($GLOBALS[\'db_test\'][\'%s\\\\%s::getAll\']))\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $GLOBALS[\'db_test\'][\'%s\\\\%s::getAll\'];\n' % (
            self.deal_namespace(table.namespace), self.class_name)
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$db = \Sooh\DB\KRedis::getInstance(\'%s\');\n' % table.config
        final_str += '\t' * 2 + '$rs = $db->getAll(\'%s\');\n' % self.table_name
        final_str += '\t' * 2 + 'if (!$rs)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$ret = [];\n'
        final_str += '\t' * 2 + 'foreach ($rs as $v)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + '$tmp = json_decode(\n'
        final_str += '\t' * 4 + '$v,\n'
        final_str += '\t' * 4 + 'true\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 3 + 'if (0 == $tmp[\'del\'])\n'
        final_str += '\t' * 3 + '{\n'
        final_str += '\t' * 4 + '$ret[] = $tmp;\n'
        final_str += '\t' * 3 + '}\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'return $ret;\n'
        final_str += '\t' * 1 + '}\n'

        return final_str

    def get_default_del_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * delete a record.\n'
        param_str = ''

        for f_name in table.primary_key.split(','):
            field_obj = table.field_list[f_name]
            func_doc_comment += '\t' + ' * @param  $%s\t%s\n' % (f_name, field_obj.desc)
            if param_str == '':
                param_str += '\t' * 2 + '$%s\n' % f_name
            else:
                param_str += '\t' * 2 + ', $%s\n' % f_name

        func_doc_comment += '\t' + ' */\n'
        func_name = 'del'

        final_str = ''
        final_str += func_doc_comment
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
        final_str += '\t' * 2 + '$key = \'\';\n'
        final_str += '\t' * 2 + '$key .= $%s;\n' % table.primary_key
        final_str += '\t' * 2 + '$curDateTime = \\Sooh\\Base\\Utils::getTime();\n'
        final_str += '\t' * 2 + '$db = \Sooh\DB\KRedis::getInstance(\'%s\');\n' % table.config
        final_str += '\t' * 2 + '$rs = $db->get(\n'
        final_str += '\t' * 3 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 3 + '$key\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if (!$rs)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$rs = json_decode(\n'
        final_str += '\t' * 3 + '$rs,\n'
        final_str += '\t' * 3 + 'true\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + '++$rs[\'verid\'];\n'
        final_str += '\t' * 2 + '$rs[\'update_time\'] = $curDateTime;\n'
        final_str += '\t' * 2 + '$rs[\'del\'] = 1;\n'
        final_str += '\t' * 2 + 'if (false === $db->set(\n'
        final_str += '\t' * 4 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 4 + '$key,\n'
        final_str += '\t' * 4 + 'json_encode($rs)\n'
        final_str += '\t' * 3 + '))\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n\n'
        final_str += '\t' * 2 + '$tableIndex = \'%s_index_%s_del\';\n' % (self.table_name, table.primary_key)
        final_str += '\t' * 2 + '$keyIndex = $rs[\'%s\'];\n' % table.primary_key
        final_str += '\t' * 2 + '$rs = $db->get(\n'
        final_str += '\t' * 3 + '$tableIndex,\n'
        final_str += '\t' * 3 + '$keyIndex\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if ($rs)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + '$rs = json_decode(\n'
        final_str += '\t' * 4 + '$rs,\n'
        final_str += '\t' * 4 + 'true\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 3 + '$tmp = $rs;\n'
        final_str += '\t' * 3 + '$rs = [];\n'
        final_str += '\t' * 3 + 'foreach($tmp as $k)\n'
        final_str += '\t' * 3 + '{\n'
        final_str += '\t' * 4 + 'if ($key !== $k)\n'
        final_str += '\t' * 4 + '{\n'
        final_str += '\t' * 5 + '$rs[] = $k;\n'
        final_str += '\t' * 4 + '}\n'
        final_str += '\t' * 3 + '}\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'if (count($rs) > 0)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $db->set(\n'
        final_str += '\t' * 4 + '$tableIndex,\n'
        final_str += '\t' * 4 + '$keyIndex,\n'
        final_str += '\t' * 4 + 'json_encode($rs)\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'else\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $db->del(\n'
        final_str += '\t' * 4 + '$tableIndex,\n'
        final_str += '\t' * 4 + '$keyIndex\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 1 + '}\n'

        return final_str

    def get_default_delreal_str(self, table):
        func_doc_comment = '\t/**\n'
        func_doc_comment += '\t * delete a record real.\n'
        param_str = ''

        for f_name in table.primary_key.split(','):
            field_obj = table.field_list[f_name]
            func_doc_comment += '\t' + ' * @param  $%s\t%s\n' % (f_name, field_obj.desc)
            if param_str == '':
                param_str += '\t' * 2 + '$%s\n' % f_name
            else:
                param_str += '\t' * 2 + ', $%s\n' % f_name

        func_doc_comment += '\t' + ' */\n'
        func_name = 'delReal'

        final_str = ''
        final_str += func_doc_comment
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
        final_str += '\t' * 2 + '$key = \'\';\n'
        final_str += '\t' * 2 + '$key .= $%s;\n' % table.primary_key
        final_str += '\t' * 2 + '$db = \Sooh\DB\KRedis::getInstance(\'%s\');\n' % table.config
        final_str += '\t' * 2 + '$rs = $db->get(\n'
        final_str += '\t' * 3 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 3 + '$key\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if (!$db->del(\n'
        final_str += '\t' * 3 + '\'%s\',\n' % self.table_name
        final_str += '\t' * 3 + '$key\n'
        final_str += '\t' * 2 + '))\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n\n'
        final_str += '\t' * 2 + 'if (!$rs)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return false;\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + '$rs = json_decode(\n'
        final_str += '\t' * 3 + '$rs,\n'
        final_str += '\t' * 3 + 'true\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + '$tableIndex = \'%s_index_%s_del\';\n' % (self.table_name, table.primary_key)
        final_str += '\t' * 2 + '$keyIndex = $rs[\'%s\'];\n' % table.primary_key
        final_str += '\t' * 2 + '$rs = $db->get(\n'
        final_str += '\t' * 3 + '$tableIndex,\n'
        final_str += '\t' * 3 + '$keyIndex\n'
        final_str += '\t' * 2 + ');\n'
        final_str += '\t' * 2 + 'if ($rs)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + '$rs = json_decode(\n'
        final_str += '\t' * 4 + '$rs,\n'
        final_str += '\t' * 4 + 'true\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 3 + '$tmp = $rs;\n'
        final_str += '\t' * 3 + '$rs = [];\n'
        final_str += '\t' * 3 + 'foreach($tmp as $k)\n'
        final_str += '\t' * 3 + '{\n'
        final_str += '\t' * 4 + 'if ($key !== $k)\n'
        final_str += '\t' * 4 + '{\n'
        final_str += '\t' * 5 + '$rs[] = $k;\n'
        final_str += '\t' * 4 + '}\n'
        final_str += '\t' * 3 + '}\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'if (count($rs) > 0)\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $db->set(\n'
        final_str += '\t' * 4 + '$tableIndex,\n'
        final_str += '\t' * 4 + '$keyIndex,\n'
        final_str += '\t' * 4 + 'json_encode($rs)\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 2 + 'else\n'
        final_str += '\t' * 2 + '{\n'
        final_str += '\t' * 3 + 'return $db->del(\n'
        final_str += '\t' * 4 + '$tableIndex,\n'
        final_str += '\t' * 4 + '$keyIndex\n'
        final_str += '\t' * 3 + ');\n'
        final_str += '\t' * 2 + '}\n'
        final_str += '\t' * 1 + '}\n'

        return final_str