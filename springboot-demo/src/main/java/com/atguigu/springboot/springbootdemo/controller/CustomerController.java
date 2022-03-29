package com.atguigu.springboot.springbootdemo.controller;

import com.atguigu.springboot.springbootdemo.bean.Customer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * 控制层
 */
//@Controller  // 标识为控制层（Spring）
@RestController   // @Controller + @ResponseBody
public class CustomerController {

    /**
     *   请求参数:
     *   1. 地址栏中的kv格式的参数
     *   2. 嵌入到地址栏中的参数
     *   3. 封装到请求体中的参数
     */

    /**
     * 3. 封装到请求体中的参数
     *
     *  http://localhost:8080/parambody
     *
     *  请求体中的参数:
     *  username=weiyunhui
     *  password=123123
     *
     * 如果请求参数名与方法的形参名不一致，需要通过@RequestParam来标识获取
     * 如果一致，可以直接映射.
     *
     * @RequestBody: 将请求体中的参数映射到对象中对应的属性上.
     *
     */
    @RequestMapping(value = "parambody" , method = RequestMethod.POST )
    public Customer parambody(@RequestBody Customer customer) {
        System.out.println("-------------------");
        return customer ;  //转换成json返回给客户端
    }

    /*
    @RequestMapping("parambody")
    public String parambody(String username , String password ){
        return "username = " + username + " , password = " + password ;
    }
     */


    /**
     *  2. 嵌入到地址栏中的参数
     *
     *  http://localhost:8080/parampath/lisi/22?address=beijing
     *
     * @PathVariable : 将请求路径中的参数映射到请求方法对应的形参上.
     */
    @RequestMapping("parampath/{username}/{age}")
    public String parampath(@PathVariable("username") String username,
                            @PathVariable("age") Integer age ,
                            @RequestParam("address") String address ){
        return "username = " + username + " , age = "+ age + " , address = " + address ;
    }


    /**
     *  1. 地址栏中的kv格式的参数
     *
     *  http://localhost:8080/paramkv?username=zhangsan&age=22
     *
     * @RequestParam： 将请求参数映射到方法对应的形参上,
     *                 如果请求参数名与方法形参名一致，可以直接进行参数值的映射,可以省略@RequestParam
     */

    @RequestMapping("paramkv")
    public String paramkv(@RequestParam("username") String name , @RequestParam("age") Integer age ){

        return "username = " + name + " , age = "+ age ;
    }





    /**
     * 客户端请求: http://localhost:8080/helloworld     hello  world  abc .....
     *
     * 请求处理方法
     *
     * @RequestMapping : 将客户端的请求与方法进行映射
     *
     * @ResponseBody : 将方法的返回值处理成字符串(json)返回给客户端
     */
    @RequestMapping("helloworld")
    //@ResponseBody
    public String helloworld(){
        return "success";
    }

    /**
     * 客户端请求: http://localhost:8080/hello   hello  world  abc .....
     *
     * 请求处理方法
     *
     * @RequestMapping : 将客户端的请求与方法进行映射
     *
     * @ResponseBody : 将方法的返回值处理成字符串(json)返回给客户端
     */
    @RequestMapping("hello")
    //@ResponseBody
    public String hello(){
        return "hello Java";
    }

}
