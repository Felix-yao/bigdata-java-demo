package com.felix.mapreduce.controller;

/**
 * @ClassName : MapReduceAction
 * @Description :
 * @Author : felix
 * @Date: 2020-12-07 16:57
 */

import com.felix.mapreduce.entity.Result;
import com.felix.mapreduce.service.MapReduceService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * MapReduce处理控制层
 * @author linhaiy
 * @date 2019.05.18
 */
@RestController
@RequestMapping("/hadoop/reduce")
public class MapReduceAction {

    @Autowired
    MapReduceService mapReduceService;

    /**
     * 单词统计(统计指定key单词的出现次数)
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "wordCount", method = RequestMethod.GET)
    @ResponseBody
    public Result wordCount(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath)
            throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return Result.error( "请求参数为空");
        }
        mapReduceService.wordCount(jobName, inputPath);
        return Result.OK( "单词统计成功");
    }
}
