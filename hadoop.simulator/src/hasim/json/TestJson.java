package hasim.json;

import hasim.json.User.Gender;
import hasim.json.User.Name;

import java.io.*;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class TestJson {

	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper=new ObjectMapper();
//		User user = mapper.readValue(new File("data/js1"), User.class);
//		System.out.println(user.toString());
//		
		
		JsonHardDisk jhdd = mapper.readValue(new File("data/json/hdd.json"),
				new TypeReference<Map<String,JsonHardDisk>>() {});
//				JsonHDD.class);
		
		System.out.println(jhdd.toString());
//		
//		Map<String,Object> userData = mapper.readValue(new File("data/js1"), Map.class);
//		
//		mapper.writeValue(new File("data/jo.out"),userData);
//		System.out.println(userData);
	}
}

class Machine {
	public enum Gender { MALE, FEMALE };

	public static class HDD {
		private double _read, _write, _capacity;

		public double get_read() {
			return _read;
		}

		public void set_read(double read) {
			_read = read;
		}

		public double get_write() {
			return _write;
		}

		public void set_write(double write) {
			_write = write;
		}

		public double get_capacity() {
			return _capacity;
		}

		public void set_capacity(double capacity) {
			_capacity = capacity;
		}

	
	}

	private Gender _gender;
	private Name _name;
	private boolean _isVerified;
	private byte[] _userImage;

	public Name getName() { return _name; }
	public boolean isVerified() { return _isVerified; }
	public Gender getGender() { return _gender; }
	public byte[] getUserImage() { return _userImage; }

	public void setName(Name n) { _name = n; }
	public void setVerified(boolean b) { _isVerified = b; }
	public void setGender(Gender g) { _gender = g; }
	public void setUserImage(byte[] b) { _userImage = b; }
	
	
	@Override
	public String toString() {
		return getName()+","+getGender()+"\n";
	}
}