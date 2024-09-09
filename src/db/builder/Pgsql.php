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
declare (strict_types=1);

namespace think\db\builder;

use think\db\Builder;
use think\db\ConflictBuilderInterface;
use think\db\Query;
use think\db\Raw;

/**
 * Pgsql数据库驱动
 */
class Pgsql extends Builder
{
    /**
     * INSERT SQL表达式
     * @var string
     */
    protected $insertSql = 'INSERT INTO %TABLE% (%FIELD%) VALUES (%DATA%) %CONFLICT%';

    /**
     * INSERT ALL SQL表达式
     * @var string
     */
    protected $insertAllSql = 'INSERT INTO %TABLE% (%FIELD%) VALUES %DATA% %CONFLICT%';

    /**
     * limit分析
     * @access protected
     * @param Query $query 查询对象
     * @param mixed $limit
     * @return string
     */
    public function parseLimit(Query $query, string $limit): string
    {
        $limitStr = '';

        if (!empty($limit)) {
            $limit = explode(',', $limit);
            if (count($limit) > 1) {
                $limitStr .= ' LIMIT ' . $limit[1] . ' OFFSET ' . $limit[0] . ' ';
            } else {
                $limitStr .= ' LIMIT ' . $limit[0] . ' ';
            }
        }

        return $limitStr;
    }

    /**
     * 获取解决插入冲突时,解决冲突的方法
     * @param array $options 数据表的设置项
     * @param string $tableName 当前表名
     * @return string 返回解决冲突的扩展语句
     */
    protected function getConflict(array $options, string $tableName): string
    {
        $conflict = '';
        if (!empty($options['conflict']) && $options['conflict'] instanceof ConflictBuilderInterface) {
            $conflict = $options['conflict']->Builder($tableName);
        }
        return $conflict;
    }

    /**
     * 字段和表名处理
     * @access public
     * @param Query $query 查询对象
     * @param mixed $key 字段名
     * @param bool $strict 严格检测
     * @return string
     */
    public function parseKey(Query $query, $key, bool $strict = false): string
    {
        if (is_int($key)) {
            return (string)$key;
        }
        if ($key instanceof Raw) {
            return $this->parseRaw($query, $key);
        }

        $key = trim($key);

        if (strpos($key, '->') && false === strpos($key, '(')) {
            // JSON字段支持
            [$field, $name] = explode('->', $key);
            $key            = '"' . $field . '"' . '->>\'' . $name . '\'';
        } elseif (strpos($key, '.')) {
            [$table, $key] = explode('.', $key, 2);

            $alias = $query->getOptions('alias');

            if ('__TABLE__' == $table) {
                $table = $query->getOptions('table');
                $table = is_array($table) ? array_shift($table) : $table;
            }

            if (isset($alias[$table])) {
                $table = $alias[$table];
            }

            if ('*' != $key && !preg_match('/[,\"\*\(\).\s]/', $key)) {
                $key = '"' . $key . '"';
            }
        }

        if (isset($table)) {
            $key = $table . '.' . $key;
        }

        return $key;
    }

    /**
     * 随机排序
     * @access protected
     * @param Query $query 查询对象
     * @return string
     */
    protected function parseRand(Query $query): string
    {
        return 'RANDOM()';
    }

    /**
     * 生成Insert SQL
     * @access public
     * @param Query $query 查询对象
     * @return string
     */
    public function insert(Query $query): string
    {
        $options = $query->getOptions();

        // 分析并处理数据
        $data = $this->parseData($query, $options['data']);
        if (empty($data)) {
            return '';
        }

        $fields = array_keys($data);
        $values = array_values($data);

        $tableName = $this->parseTable($query, $options['table']);
        $conflict = $this->getConflict($options, $tableName);

        return str_replace(
            ['%TABLE%', '%FIELD%', '%DATA%', '%CONFLICT%'],
            [
                $tableName,
                implode(' , ', $fields),
                implode(' , ', $values),
                $conflict,
            ],
            $this->insertSql
        );
    }


    /**
     * 生成insertall SQL
     * @access public
     * @param Query $query 查询对象
     * @param array $dataSet 数据集
     * @return string
     */
    public function insertAll(Query $query, array $dataSet): string
    {
        $options = $query->getOptions();

        // 获取绑定信息
        $bind = $query->getFieldsBindType();

        // 获取合法的字段
        if (empty($options['field']) || '*' == $options['field']) {
            $allowFields = array_keys($bind);
        } else {
            $allowFields = $options['field'];
        }

        $fields = [];
        $values = [];

        foreach ($dataSet as $k => $data) {
            $data = $this->parseData($query, $data, $allowFields, $bind);

            $values[] = '( ' . implode(',', array_values($data)) . ')';

            if (!isset($insertFields)) {
                $insertFields = array_keys($data);
            }
        }

        foreach ($insertFields as $field) {
            $fields[] = $this->parseKey($query, $field);
        }

        $tableName = $this->parseTable($query, $options['table']);
        $conflict = $this->getConflict($options, $tableName);

        return str_replace(
            ['%TABLE%', '%FIELD%', '%DATA%', '%CONFLICT%'],
            [
                $tableName,
                implode(' , ', $fields),
                implode(' , ', $values),
                $conflict,
            ],
            $this->insertAllSql);
    }

}
