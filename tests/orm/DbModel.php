<?php

namespace tests\orm;

use tests\Base;
use tests\model\UserModel;


class DbModel extends Base
{


    public function testSelect(){
        $model = new UserModel();
        $result = $model->select();
        $this->assertIsArray($result, 'select不是数组');
        $this->assertIsArray(current($result), 'select不是数组');
        $this->assertCount(10, $result);
    }
    public function testFind(){
        $model = new UserModel();
        $result = $model->where('id',1)->find();
        $this->assertIsArray($result, 'select不是数组');
        $this->assertCount(5, $result);
    }

}



