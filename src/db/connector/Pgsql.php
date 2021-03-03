<?php
// +----------------------------------------------------------------------
// | ThinkPHP [ WE CAN DO IT JUST THINK ]
// +----------------------------------------------------------------------
// | Copyright (c) 2006~2019 http://thinkphp.cn All rights reserved.
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: liu21st <liu21st@gmail.com>
// +----------------------------------------------------------------------

namespace think\db\connector;

use PDO;
use think\db\BaseQuery;
use think\db\PDOConnection;

/**
 * Pgsql数据库驱动
 */
class Pgsql extends PDOConnection
{

    /**
     * 默认PDO连接参数
     * @var array
     */
    protected $params = [
        PDO::ATTR_CASE => PDO::CASE_NATURAL,
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_ORACLE_NULLS => PDO::NULL_NATURAL,
        PDO::ATTR_STRINGIFY_FETCHES => false,
    ];

    /**
     * 解析pdo连接的dsn信息
     * @access protected
     * @param array $config 连接信息
     * @return string
     */
    protected function parseDsn(array $config): string
    {
        $dsn = 'pgsql:dbname=' . $config['database'] . ';host=' . $config['hostname'];

        if (!empty($config['hostport'])) {
            $dsn .= ';port=' . $config['hostport'];
        }

        return $dsn;
    }

    /**
     * 取得数据表的字段信息
     * @access public
     * @param string $tableName
     * @return array
     */
    public function getFields(string $tableName): array
    {
        [$tableName] = explode(' ', $tableName);
        $sql = 'select fields_name as "field",fields_type as "type",fields_not_null as "null",fields_key_name as "key",fields_default as "default",fields_default as "extra" from table_msg(\'' . $tableName . '\');';

        $pdo = $this->getPDOStatement($sql);
        $result = $pdo->fetchAll(PDO::FETCH_ASSOC);
        $info = [];

        if (!empty($result)) {
            foreach ($result as $key => $val) {
                $val = array_change_key_case($val);

                $info[$val['field']] = [
                    'name' => $val['field'],
                    'type' => $val['type'],
                    'notnull' => (bool)('' !== $val['null']),
                    'default' => $val['default'],
                    'primary' => !empty($val['key']),
                    'autoinc' => (0 === strpos($val['extra'], 'nextval(')),
                ];
            }
        }

        return $this->fieldCase($info);
    }

    /**
     * 取得数据库的表信息
     * @access public
     * @param string $dbName
     * @return array
     */
    public function getTables(string $dbName = ''): array
    {
        $sql = "select tablename as Tables_in_test from pg_tables where  schemaname ='public'";
        $pdo = $this->getPDOStatement($sql);
        $result = $pdo->fetchAll(PDO::FETCH_ASSOC);
        $info = [];

        foreach ($result as $key => $val) {
            $info[$key] = current($val);
        }

        return $info;
    }

    /**
     * 插入记录
     * @access public
     * @param BaseQuery $query 查询对象
     * @param boolean $getLastInsID 返回自增主键
     * @return mixed
     */
    public function insert(BaseQuery $query, bool $getLastInsID = false)
    {
        // 分析查询表达式
        $options = $query->parseOptions();

        // 生成SQL语句
        $sql = $this->builder->insert($query);

        // 执行操作
        $result = '' == $sql ? 0 : $this->pdoExecute($query, $sql, $query->getBind());

        if ($result) {
            $pk = $query->getAutoInc();
            $lastInsId = '';
            if ($pk) {
                $sequence = $options['sequence'] ?? null;
                $lastInsId = $this->getLastInsID($query, $sequence);
            }
            $data = $options['data'];
            if ($lastInsId && $pk) {
                $data[$pk] = $lastInsId;
            }

            $query->setOption('data', $data);

            $this->db->trigger('after_insert', $query);

            if ($getLastInsID && $lastInsId) {
                return $lastInsId;
            }
        }

        return $result;
    }

    protected function supportSavepoint(): bool
    {
        return true;
    }
}
