package com.atguigu.gmall.udtf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2020/8/1 14:07
 */
@Description(name = "explode_json_array", value = " - this function can explode jsonArray ")
public class ExplodeJsonArray extends GenericUDTF {
    /*
    作用:
        1. 对输入做检测
        2. 返回期望类型数据的检测器
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 1. 对输入做检测
        // lateral view explode_json_array(get_json_object(line, '$.actions'))
        // 1.1 参数个数必须满足
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        if(inputFields.size() != 1){
            throw new UDFArgumentException("explode_json_array 的参数格式必须是 1, 你传的参数不对");
        }
        // 1.2 参数类型必须是字符串
        ObjectInspector oi = inputFields.get(0).getFieldObjectInspector();
        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE || !"string".equals(oi.getTypeName())) {
            throw new UDFArgumentException("explode_json_array 参数类型必须是string, 请检测你的类型...");
        }
        // 2. 返回期望类型数据的检测器
        List<String> names = new ArrayList<>();
        names.add("action");
        List<ObjectInspector> ois = new ArrayList<>();
        ois.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(names, ois);
    }
    /*
        处理数据  [{}, {}, {}, ...]
            forward();
     */
    @Override
    public void process(Object[] args) throws HiveException {
        // 给你一个json字符串, 解析出来, 然后把每个一个json对象forward出现
        String jsonArrayString = args[0].toString();  // 传入的数据

        JSONArray arr = new JSONArray(jsonArrayString);

        for (int i = 0; i < arr.length(); i++) {
            String col = arr.getString(i);

            String[] cols = new String[1];
            cols[0] = col;
            forward(cols);  // 为什么需要数组? 因为将来炸裂的结果可能会有多列, 可以用数组表示多列数据
        }
    }
    /*
    关闭资源
    不用实现
     */
    @Override
    public void close() throws HiveException {

    }
}
