package com.gy.hbase.controller;

import com.gy.hbase.services.HBaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * created by yangyu on 2019-11-08
 */
@RestController
@RequestMapping(value = "/import")
public class ImportController {

    @Autowired
    private HBaseService hBaseService;

    @PostMapping(value = "/test1",produces = "application/json;charset=UTF-8")
    @ResponseBody
    public void test1(@RequestBody Map<String,String> paramMap) throws Exception{
        hBaseService.putDataH(paramMap.get("tableName"),
                paramMap.get("rowKey"),paramMap.get("family"),
                paramMap.get("qualifier"),paramMap.get("value"));
    }

}
