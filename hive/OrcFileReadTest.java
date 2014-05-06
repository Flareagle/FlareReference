package com.baidu.rigelci.ddc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecordSerDe;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.pig.PigHCatUtil;
import org.apache.pig.ResourceSchema;

public class OrcFileReadTest {
	public static void main(String[] args) throws IOException, SerDeException {
		Configuration conf=new Configuration();
		PigHCatUtil phutil = new PigHCatUtil();
		Job job=new Job(new Configuration());
		Table table = phutil.getTable("analyse_db.contline_revenue_day_orc", PigHCatUtil.getHCatServerUri(job),PigHCatUtil.getHCatServerPrincipal(job));
		HCatSchema hcatTableSchema = HCatUtil.getTableSchemaWithPtnCols(table);
		ResourceSchema rsSchema=PigHCatUtil.getResourceSchema(hcatTableSchema);
		// TODO Auto-generated method stub
        //prodline_id>100 
		TypeInfo longType=TypeInfoFactory.longTypeInfo;
		TypeInfo boolType=TypeInfoFactory.booleanTypeInfo;
		ExprNodeDesc constant=new ExprNodeConstantDesc(longType,100);
		ExprNodeDesc colExpr=new ExprNodeColumnDesc(longType,"prodline_id","contline_revenue_day_orc",false);
		List<ExprNodeDesc> child=new ArrayList<ExprNodeDesc>();
		child.add(colExpr);
		child.add(constant);
		ExprNodeDesc exp1=new ExprNodeGenericFuncDesc(boolType,
				new GenericUDFOPGreaterThan(),
				child
				);
		conf.set("hive.io.file.readcolumn.ids", "0,1,7,9");  //列裁剪
		//以下两个是做谓词下推   
		conf.set("hive.io.filter.expr.serialized", Utilities.serializeExpression(exp1));
		conf.set("hive.io.file.readcolumn.names", "contract_line_id,st_date,prodline_id,alb_cust_id");
		String inputFile="/home/work/data/hive/warehouse/analyse_db.db/contline_revenue_day_orc/pdate=2014-03-09/000000_0";
		conf.set("mapred.input.dir",inputFile);
		OrcInputFormat input=new OrcInputFormat();
		InputSplit[] splits=input.getSplits(new JobConf(conf), 1);
		Path path=new Path(inputFile);
		Reader fileReader=OrcFile.createReader(path.getFileSystem(conf),path);
		StructObjectInspector insp = (StructObjectInspector) fileReader.getObjectInspector();
		int fildNum=insp.getAllStructFieldRefs().size();
		for(InputSplit split:splits){
			RecordReader<NullWritable, OrcStruct> reader=input.getRecordReader(split, new JobConf(conf), Reporter.NULL);
			NullWritable key=reader.createKey();
			OrcStruct value=reader.createValue();
			while(reader.next(key, value)){
				for(int i=0;i<fildNum;i++){
					StructField fref = insp.getAllStructFieldRefs().get(i);
					Object data=insp.getStructFieldsDataAsList(value).get(i);
					Object obj=HCatRecordSerDe.serializeField(data,fref.getFieldObjectInspector());
					System.out.print(obj==null?"null":obj.getClass()+"---"+obj);
				}
				System.out.println();
			}
			reader.close();
		}
	}

}
