package org.apache.iotdb;

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OriginReadClass {
    private static final int CONCURRENCY = 8;

    private static final int TSFILESIZE = 20;

    private static final int TRANSFORMSIZE = 500000;

    static String inputFilePath = "/Users/beyyes/shangfei/small";

    public static void main(String[] args) {

    }

    public static void main2(String[] args)
            throws InterruptedException, IOException {
        File folder = new File(inputFilePath);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".txt"));
        long startTime = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENCY + 1);
        if(files==null){
            System.out.println("No files in "+ inputFilePath);
            return;
        }
        String[] fileAbsolutePaths = new String[files.length];
        String[] fileNames = new String[files.length];
        for (int i = 0; i < files.length; i++) {
            fileAbsolutePaths[i] = files[i].getAbsolutePath();
            fileNames[i] = files[i].getName().replace(".txt","");
        }
        List<Callable<Void>> taskList = new ArrayList<>();
        List<List<String>>readLines = new ArrayList<>();
        List<List<MeasurementSchema>>schemaLists = new ArrayList<>();
        long sumReadBegin = System.nanoTime();
        for (int i=0;i<fileAbsolutePaths.length;i++) {
            try {
                // 读文件
                long startRead = System.nanoTime();
                List<String> lines = Files.readAllLines(Paths.get(fileAbsolutePaths[i]));
                long endRead = System.nanoTime();
                System.out.println("Read " + lines.size() + " lines from " + fileAbsolutePaths[i] + " in " + (endRead - startRead) / 1000000 + " ms");
                readLines.add(lines);
                List<MeasurementSchema> schemaList = getType(lines.get(0), lines.get(1));
                schemaLists.add(schemaList);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        long sumReadEnd = System.nanoTime();
        System.out.println("Read all files in "+(sumReadEnd-sumReadBegin)/1000000+" ms");

        long sumTransformBegin = System.nanoTime();
        //处理时间戳
        for(int i=0;i<readLines.size();i++){
            String dataTime = fileNames[i].split("-")[4];
            DateTimeFormatter inputFileFormat = DateTimeFormatter.ofPattern("yyMMdd").withZone(ZoneId.of("GMT+8"));
            long dataTimestamp = LocalDate.parse(dataTime, inputFileFormat).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()*1000;
            if(readLines.get(i).size()>2*TRANSFORMSIZE){
                List<List<String>>subLines = splitListByLine(readLines.get(i));
                List<Thread>threads = new ArrayList<>();
                ConcurrentHashMap<Integer,List<String>>results = new ConcurrentHashMap<>();
                for(int index = 0;index<subLines.size();index++){
                    List<String> subLine = subLines.get(index);
                    int finalIndex = index;
                    Thread thread = new Thread(()->{
                        List<String>result = new ArrayList<>();
                        for (String s : subLine) {
                            String[] values = s.split("\t");
                            long nowTime = dataTimestamp + tryParse(values[0]);
                            values[0] = String.valueOf(nowTime);
                            result.add(String.join("\t", values));
                        }
                        results.put(finalIndex,result);
                    });
                    thread.start();
                    threads.add(thread);
                }
                for(Thread thread:threads){
                    thread.join();
                }
                List<String>finalResult = new ArrayList<>();
                for(int j=0;j<subLines.size();j++){
                    finalResult.addAll(results.get(j));
                }
                readLines.set(i,finalResult);
            }
            else{
                List<String>result = new ArrayList<>();
                for(int j=1;j<readLines.get(i).size();j++){
                    String[] values = readLines.get(i).get(j).split("\t");
                    long nowTime = dataTimestamp + tryParse(values[0]);
                    values[0] = String.valueOf(nowTime);
                    result.add(String.join("\t", values));
                }
                readLines.set(i,result);
            }
        }
        long sumTransformEnd = System.nanoTime();
        System.out.println("Transform all files in "+(sumTransformEnd-sumTransformBegin)/1000000+" ms");

        for(int i=0;i<readLines.size();i++){
            List<List<String>>subLines = splitListBySize(readLines.get(i));
            for(int j=0;j<subLines.size();j++){
                int finalI = i;
                int finalJ = j;
                taskList.add(() -> {
                    TsfileThread tmp  = new TsfileThread(schemaLists.get(finalI), subLines.get(finalJ), fileNames[finalI], finalJ);
                    tmp.call();
                    return null;
                });
            }
        }
        System.out.println("Begin to write tsfiles");
        long tsfileWrite = System.nanoTime();
        executorService.invokeAll(taskList);
        long tsfileWriteEnd = System.nanoTime();
        System.out.println("Write tsfiles in "+(tsfileWriteEnd-tsfileWrite)/1000000+" ms");
        executorService.shutdown();
        long endTime = System.nanoTime();
        System.out.println("Total time: " + (endTime - startTime) / 1000000 + " ms");
    }
    //视为只有int double text 三种类型，并且不考虑time列   要求第一行不能有缺，不然会识别为string类型
    public static List<MeasurementSchema> getType(String line1, String line2){
        String[] columns = line1.split("\t");
        String[] parts = line2.split("\t");
        List<MeasurementSchema> schemaList = new ArrayList<>();
        for(int i=1;i<columns.length;i++){
            try{
                Long.parseLong(parts[i]);
                schemaList.add(new MeasurementSchema(columns[i], TSDataType.INT64, TSEncoding.TS_2DIFF, CompressionType.SNAPPY));
            }catch (NumberFormatException e){
                try{
                    Double.parseDouble(parts[i]);
                    schemaList.add(new MeasurementSchema(columns[i], TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY));
                }catch (NumberFormatException e1){
                    //string
                    schemaList.add(new MeasurementSchema(columns[i], TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.SNAPPY));
                }
            }
        }
        return schemaList;
    }
    public  static long tryParse(String text) {
        //微妙格式
        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("HH:mm:ss:SSSSSS");
        //毫秒格式
        DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("HH:mm:ss:SSS");
        //两种格式都转化为微妙的格式去存储
        try {
            LocalTime localTime = LocalTime.parse(text, formatter1);
            return localTime.toNanoOfDay() / 1000L;
        } catch (DateTimeParseException e) {
            try {
                LocalTime localTime = LocalTime.parse(text, formatter2);
                return localTime.toNanoOfDay() / 1000L;
            } catch (DateTimeParseException e1) {
                System.out.println("Error input time format: " + text + ", use 0 instead.");
                return 0;
            }
        }
    }
    private static <T> List<List<T>> splitListByLine(List<T> list) {
        List<List<T>> subLists = new ArrayList<>();
        for (int i = 1; i < list.size(); i += TRANSFORMSIZE) {
            subLists.add(list.subList(i, Math.min(i + TRANSFORMSIZE, list.size())));
        }
        return subLists;
    }

    private static <T> List<List<T>> splitListBySize(List<T> list) {
        List<List<T>> subLists = new ArrayList<>();
        int batchSize = list.size()/ TSFILESIZE;
        int line=1;
        for (int i = 0; i < TSFILESIZE -1; i++,line+=batchSize){
            subLists.add(list.subList(line, line+batchSize));
        }
        subLists.add(list.subList(line, list.size()));
        return subLists;
    }

    static class TsfileThread implements Callable<Void>{
        Tablet tablet;
        List<MeasurementSchema> schemaList;
        List<String> values;

        TsFileWriter tsFileWriter;
        String fileName;

        String outPath;
        String device;
        int index;

        List<MeasurementSchema> writeMeasurementSchemas;

        public TsfileThread(
                List<MeasurementSchema> schemaList, List<String> values, String fileName, int index)
                throws IOException, WriteProcessException {
            this.schemaList = schemaList;
            this.fileName = fileName;
            this.index = index;
            this.values = values;
            device = "root.import."+fileName.split("-")[7];

            outPath = "/Users/beyyes/shangfei/tsfiles"+fileName+"-"+index+".tsfile";
//            outPath = "C:\\Users\\lyf\\Desktop\\iotdbDeploy\\SFdeploy\\test\\afterData\\"+fileName+"-"+index+".tsfile";
            File f = FSFactoryProducer.getFSFactory().getFile(outPath);
            if (f.exists() && !f.delete()) {
                throw new RuntimeException("can not delete " + f.getAbsolutePath());
            }
            tsFileWriter = new TsFileWriter(f);
            tsFileWriter.registerAlignedTimeseries(new Path(device),this.schemaList);
            writeMeasurementSchemas = new ArrayList<>();
            writeMeasurementSchemas.addAll(this.schemaList);
        }

        public Void call() throws IOException {
            long startTime = System.nanoTime();
            try{
                //tablet 的size过大就会失败 （已知7500000 会失败
                tablet = new Tablet(device, writeMeasurementSchemas,Math.min(60000000/writeMeasurementSchemas.size(),100000));
                for (int i = 0; i < values.size(); i++) {
                    int row = tablet.rowSize++;
                    String[] sparts = values.get(i).split("\t");
                    tablet.timestamps[row] = Long.parseLong(sparts[0]);
                    for(int j=1;j<sparts.length;j++){
                        if (sparts[j].equals("NA")|| sparts[j].equals("")) {
                            tablet.addValue(
                                    writeMeasurementSchemas.get(j-1).getMeasurementId(), row, null);
                            continue;
                        }
                        switch (writeMeasurementSchemas.get(j-1).getType()) {
                            case INT64:
                                tablet.addValue(
                                        writeMeasurementSchemas.get(j-1).getMeasurementId(), row, Long.parseLong(sparts[j]));
                                break;
                            case DOUBLE:
                                tablet.addValue(
                                        writeMeasurementSchemas.get(j-1).getMeasurementId(), row, Double.parseDouble(sparts[j]));
                                break;
                            case TEXT:
                                tablet.addValue(
                                        writeMeasurementSchemas.get(j-1).getMeasurementId(), row, sparts[j]);
                                break;
                            default:
                                System.out.println("this type of data is not supported");
                        }
                    }
                    if (tablet.rowSize == tablet.getMaxRowNumber()) {
                        tsFileWriter.writeAligned(tablet);
                        tablet.reset();
                    }
                }
                if (tablet.rowSize != 0) {
                    tsFileWriter.writeAligned(tablet);
                    tablet.reset();
                }
            } catch (Throwable e) {
                e.printStackTrace();
                System.out.println("Error in row "+tablet.rowSize + " of file "+outPath);
                return null;
            } finally {
                long endTime = System.nanoTime();
                System.out.println("Insert file " +outPath+ " using " + (endTime-startTime)/1000000 + " ms");
                tsFileWriter.close();
            }
            return null;
        }
    }

}
