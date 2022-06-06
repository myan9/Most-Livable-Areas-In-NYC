import java.io.IOException;

import javax.sound.midi.SysexMessage;

import org.apache.hadoop.io.IntWritable ; 
import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.mapreduce.Mapper ; 

public class dataFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private boolean isYearWithinRange(int input) {
        return (2010 <= input && input <= 2018);
    }

    private boolean isValidZIP(String input) {
        
        if(input.isEmpty()|| input.equals("NA") || input.equals("N") || input.equals("N/A") || 
            input.equals("0") || input.equals("1175")) {
            return false;
        }

        try {
            int zip_numeric = Integer.parseInt(input);
            if(10001 <= zip_numeric && zip_numeric <= 11439) {
                return true;
            }
        } catch (NumberFormatException nfe) {
            return false;
        }

        return false;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
		String line = value.toString(), filtered_record = "", status = "";

		String[] recordTokenArray = line.split(",", -1);
        // Filter by Token number
        boolean print_once = false;
        if (recordTokenArray.length == 41) {
            // System.out.println(Integer.toString(year_opened) + " " + Integer.toString(year_closed));
            // filtered_record = String.join("|", recordTokenArray);
            if(!recordTokenArray[1].isEmpty() && !recordTokenArray[2].isEmpty()) {
                int year_opened = Integer.parseInt(recordTokenArray[1].substring(6, 10)), 
                year_closed = Integer.parseInt(recordTokenArray[2].substring(6, 10));
                // Filter by Year
                if(isYearWithinRange(year_opened) && isYearWithinRange(year_closed)) {
                    // System.out.printf("%d %d\n", year_opened, year_closed);
                    // Filter by Completion Status
                    
                    // System.out.print(String.join(",",recordTokenArray[18], recordTokenArray[19], recordTokenArray[20]));
                    // System.out.println();
                    if(recordTokenArray[19].equals("Closed") && !recordTokenArray[25].equals("Unspecified")) {
                        if(isValidZIP(recordTokenArray[8])) {
                            // Filter by Valid ZIP Code
                            String unique_id = recordTokenArray[0], complaint_type = recordTokenArray[5],
                                location_type = recordTokenArray[7], incident_zip = recordTokenArray[8],
                                city = recordTokenArray[16];
                            
                            if(!unique_id.isEmpty() && !complaint_type.isEmpty() && !location_type.isEmpty() && !incident_zip.isEmpty() && !incident_zip.equals("N/A") && !city.isEmpty()) {
                                filtered_record = String.join("|",
                                    recordTokenArray[0], // unique id
                                    recordTokenArray[1], // date created
                                    recordTokenArray[2], // date ended
                                    recordTokenArray[5], // complaint type
                                    recordTokenArray[7], // location type
                                    recordTokenArray[8], // incident zip
                                    recordTokenArray[16] // city
                                );
                                System.out.println(filtered_record);
                                context.write(new Text(filtered_record), NullWritable.get());
                            }
                        }
                    }
                }
            }
        }
    }
}
