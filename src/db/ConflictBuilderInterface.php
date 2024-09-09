<?php

namespace think\db;

/**
 * 用于
 */
interface ConflictBuilderInterface
{
    /**
     * 生成
     * @param string $TableName
     * @return string
     */
    public function Builder(string $TableName): string;

}
