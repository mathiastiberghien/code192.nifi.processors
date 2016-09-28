package code192.nifi.processors;

import com.google.gson.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.poi.ss.usermodel.*;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;


import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * Created by Administrator on 9/13/2016.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@TriggerSerially
@Tags({"SQL","Excel"})
@CapabilityDescription("Process dedicated to line by line extraction of an Excel file to push data on a SQL database." +
        "The user configures the file path, The DBCP connection, the target table, the row start index, the mapping between excel columns and the targe table  columnms, and the primary keys" +
        "The processor generates a flowFile for each failed query and a Json representation of the file when the operation suceed.")
@WritesAttributes({@WritesAttribute(attribute="filename",description = "JSON file name will have the name of the source file with .json extension. Fail flow files will have the primary keys separated with '_'.txt")})
public class ExcelToSQL extends AbstractProcessor {

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final AllowableValue Insert = new AllowableValue("1","Insert","Try to insert each line in the target table");
    public static final AllowableValue Update = new AllowableValue("2","Update", "Try to update each line in the target table");
    public static final AllowableValue UpdateInsert = new AllowableValue("3","Update or Insert", "Try to update each line and insert it if the line not exists in the target table");

    public static final PropertyDescriptor Excel_Path = new PropertyDescriptor.Builder()
            .name("Excel file path")
            .description("Full path of the source Excel file")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor Start_Index = new PropertyDescriptor.Builder()
            .name("Start index row")
            .description("The index (1 = first line) where the data read should start")
            .defaultValue("1")
            .addValidator(StandardValidators.createLongValidator(1,50000,true))
            .required(true)
            .build();

    public static final PropertyDescriptor UpdateMode = new PropertyDescriptor.Builder()
            .name("Update strategy")
            .description("The update strategy of the target table. Can be 'Insert', 'Update' or 'Update or Insert'. 'Update or Insert' means that the row will be create if not exists, updated otherwise.")
            .defaultValue("3")
            .allowableValues(Insert,Update,UpdateInsert)
            .required(true)
            .build();

    public static final PropertyDescriptor Column_Mapping = new PropertyDescriptor.Builder()
            .name("Columns mapping")
            .description("List separated with ';' of the Excel column indexes to read with the mapped target column. The index (1 = first column) and the column name are separated with ':'. Eg: 1:column_A;2:column_B;5:column_C")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PrimaryKeys = new PropertyDescriptor.Builder()
            .name("Primary keys")
            .description("List of primary keys or unique identifiers of the target table separated with ';'. Eg: column_A;column_B")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DataTable = new PropertyDescriptor.Builder()
            .name("Table")
            .description("The target table name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor Connexion = new PropertyDescriptor.Builder()
            .name("Connection")
            .description("DBCP Connection")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();


    public static final Relationship FAIL = new Relationship.Builder()
            .name("fail")
            .description("Each line wich fails to update the target table will generate a flow file using primary keys as name")
            .autoTerminateDefault(true)
            .build();

    public static final Relationship JSON = new Relationship.Builder()
            .name("sucess")
            .description("When the update suceeds, a JSON representation of the data is generated")
            .autoTerminateDefault(true)
            .build();

    private final HashMap<Integer,String> columnMapping = new HashMap<>();
    private final HashSet<String> primaryKeys = new HashSet<>();


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile inputF = session.get();
        if(inputF!=null)session.remove(inputF);

        final ProcessorLog log = this.getLogger();
        String excel_path = context.getProperty(Excel_Path).getValue();
        String dataTable = context.getProperty(DataTable).getValue();
        DBCPService connexion = context.getProperty(Connexion).asControllerService(DBCPService.class);
        getColumn_Mapping(context);
        int start_index = Math.max(0, context.getProperty(Start_Index).asInteger() - 1);
        int i = start_index;

        int updateMode = context.getProperty(UpdateMode).asInteger();
        boolean isInsert = (updateMode & 1) == 1;
        boolean isUpdate = (updateMode & 2) == 2;


        if (connexion != null) {

            try {

                File f = new File(excel_path);
                Workbook wb = WorkbookFactory.create(f);
                Sheet sheet = wb.getSheetAt(wb.getActiveSheetIndex());

                Connection conn = connexion.getConnection();

                if (conn != null) {
                    Statement stmt = conn.createStatement();
                    ResultSetMetaData metaData = stmt.executeQuery(String.format("Select top 1 * from [%s]", dataTable)).getMetaData();

                    HashMap<String, Integer> dataTypes = new HashMap<>();
                    for (int ind = 1; ind <= metaData.getColumnCount(); ind++) {
                        dataTypes.put(metaData.getColumnName(ind).toLowerCase(), metaData.getColumnType(ind));
                    }

                    Row r = sheet.getRow(i);
                    final JsonArray data = new JsonArray();
                    while (r != null) {
                        JsonObject jItem = new JsonObject();
                        for (int idx : this.columnMapping.keySet()
                                ) {
                            String columnName = columnMapping.get(idx);
                            jItem.addProperty(columnName,getJSONCellValue(columnName, getCellValue(r.getCell(idx - 1)),dataTypes) );

                        }
                        data.add(jItem);
                        final String sql = getSqlQuery(jItem, dataTable, dataTypes,isInsert,isUpdate);


                        i++;
                        r = sheet.getRow(i);

                        try {
                            stmt.executeUpdate(sql);

                        } catch (SQLException ex) {
                            log.error(String.format("There was an error executing update for '%s': %s", getPrimaryValues(jItem), ex.getMessage()));
                            handleQueryError(jItem, session, sql);
                        }



                    }
                    stmt.close();
                    wb.close();

                    final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
                    final JsonObject result = new JsonObject();
                    result.add("data", data);

                    FlowFile flowFile;

                    flowFile = session.create();
                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream outputStream) throws IOException {
                            outputStream.write(gson.toJson(result).getBytes());
                        }
                    });
                    flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), String.format("%s.json", FilenameUtils.removeExtension(f.getName())));

                    session.transfer(flowFile, JSON);

                    session.commit();
                }


            } catch (Exception e) {
                e.printStackTrace();
                log.error(String.format("Error processing file %s: %s", excel_path, e.getMessage()));
            }
        } else log.error("Connection was not configured properly");


    }

    private void handleQueryError(JsonObject item, ProcessSession session,final String query)
    {
        String primaryValues = getPrimaryValues(item);
        String name = primaryValues.replace(",","_");
        FlowFile flowFile;
        flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream outputStream) throws IOException {
                outputStream.write(query.getBytes());
            }
        });
        flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), name + ".txt");
        session.transfer(flowFile, FAIL);
    }

    private String getCellValue(Cell cell)
    {
        switch(cell.getCellType())
        {
            case Cell.CELL_TYPE_STRING:return cell.getStringCellValue();
            case Cell.CELL_TYPE_BLANK:return "NULL";
            case Cell.CELL_TYPE_BOOLEAN:return Boolean.toString(cell.getBooleanCellValue());
            case Cell.CELL_TYPE_ERROR:return Byte.toString( cell.getErrorCellValue());
            case Cell.CELL_TYPE_FORMULA:return cell.getCellFormula();
            case Cell.CELL_TYPE_NUMERIC: return Double.toString(cell.getNumericCellValue());

        }
        return cell.getStringCellValue();
    }

    private String getPrimaryValues(JsonObject jItem)
    {
        StringBuilder sb = new StringBuilder();
        for (String s:this.primaryKeys
                ) {
            sb.append(String.format("%s,",jItem.get(s).getAsString()));


        }
        sb.setLength(sb.length()-1);

        return sb.toString();
    }

    private void getColumn_Mapping(ProcessContext context) {
        String mappingString = context.getProperty(Column_Mapping).getValue();
        String[] columnsInfo = mappingString.split(";");
        String[] primaryKeys = context.getProperty(PrimaryKeys).getValue().split(";");

        for (String column : columnsInfo
                ) {
            String[] infos = column.split(":", 2);
            if (infos.length == 2) {
                try {
                    int i = Integer.parseInt(infos[0]);
                    if (i <= 0)
                        getLogger().error(String.format("Fail to read column mapping: index '%d' should be greater than 0",i));
                    else {
                        columnMapping.put(i, infos[1]);
                    }


                } catch (NumberFormatException ex) {
                    getLogger().error(String.format("Fail to read column mapping: index '%s' is not valid", infos[0]));
                }


            } else {
                getLogger().error(String.format("Fail to read column mapping: colunm info '%s' is not valid", column));
            }

        }

        for (String key : primaryKeys
                ) {
            if (columnMapping.values().contains(key)) {
                this.primaryKeys.add(key);
            } else
                getLogger().error(String.format("The following key was not present in column mapping and will be ignored: %s", key));

        }
    }

    private String getSqlQuery(JsonObject jItem,String dataTable,HashMap<String,Integer> dataTypes,boolean isInsert, boolean isUpdate)
    {
        StringBuilder sb = new StringBuilder();
        if(isUpdate)
        {
            sb.append(String.format("Update [%s] set ",dataTable));
            for(String s : columnMapping.values())
            {
                if(!primaryKeys.contains(s))
                {
                    sb.append(String.format("[%s]=%s, ",s, getSQLCellValue(s,jItem.get(s).getAsString(),dataTypes) ));
                }
            }
            sb.setLength(sb.length()-2);
            sb.append(" Where ");
            for(String s:primaryKeys)
            {
                sb.append(String.format("[%s]=%s and ",s, getSQLCellValue(s,jItem.get(s).getAsString(),dataTypes)));
            }
            sb.setLength(sb.length()-5);
            if(isInsert)
            {
                sb.append(" If @@ROWCOUNT = 0 ");
            }

        }
        if(isInsert)
        {
            sb.append(String.format("Insert into [%s] (",dataTable));

            for(String s:columnMapping.values())
            {
                sb.append(String.format("[%s],",s));
            }
            sb.setLength(sb.length()-1);
            sb.append(") values (");
            for(String s:columnMapping.values())
            {
                sb.append(String.format("%s,", getSQLCellValue(s,jItem.get(s).getAsString(),dataTypes)));
            }
            sb.setLength(sb.length()-1);
            sb.append(")");
        }


        return sb.toString();

    }

    private String getJSONCellValue(String columnName,String rawValue,HashMap<String,Integer> dataTypes)
    {
        String result = rawValue;
        if(result.equals("NULL"))return result;
        if(dataTypes.keySet().contains(columnName.toLowerCase()))
        {
            switch(dataTypes.get(columnName.toLowerCase()))
            {
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP: Calendar c = org.apache.poi.ss.usermodel.DateUtil.getJavaCalendar(Double.parseDouble(rawValue));
                    result =  Long.toString(c.getTimeInMillis()) ;break;
                case Types.BIGINT:
                case Types.BIT:
                case Types.INTEGER: result = Long.toString(Math.round(Double.valueOf(result)));

            }
        }
        return result;
    }

    private String getSQLCellValue(String columnName, String rawValue, HashMap<String,Integer> dataTypes)
    {
        String result = rawValue;
        if(result.equals("NULL"))return result;
        if(dataTypes.keySet().contains(columnName.toLowerCase()))
        {
            switch(dataTypes.get(columnName.toLowerCase()))
            {
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    result =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(Long.valueOf(rawValue))) ;break;

            }
        }

        return String.format("N'%s'",result.replace("'","''"));
    }



    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(Excel_Path);
        properties.add(Connexion);
        properties.add(DataTable);
        properties.add(UpdateMode);
        properties.add(Start_Index);
        properties.add(Column_Mapping);
        properties.add(PrimaryKeys);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(FAIL);
        relationships.add(JSON);
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }


}

