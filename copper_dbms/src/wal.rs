use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::Read;
struct Wal
{
    size:usize,
    max_size:usize,
    file: File,
}

impl Wal{

pub fn init(&self)->Wal
{
     let mut wal = File::create("wal.txt")
   .expect("Error encountered while creating file!");

      let mut data_file = OpenOptions::new()
        .append(true)
        .open(self.file)
        .expect("cannot open file");

    // Write to a file
    data_file
        .write(self.max_size.to_string().as_bytes())
        .expect("write failed");

    let res=Wal
    {
       size:0,
       max_size:10,
       file:wal,


    };
   return res;
}

pub fn set(&mut self,key:u8,value:u8)->()
{
    let mut data_file = OpenOptions::new()
        .append(true)
        .open(self.file)
        .expect("cannot open file");

    // Write to a file
    data_file
        .write(key.to_string().as_bytes())
        .expect("write failed");

    data_file
        .write(value.to_string().as_bytes())
        .expect("write failed");

    data_file
        .write("0".as_bytes())
        .expect("write failed");

}

pub fn get(&self,key:u8,value:u8)->(u8,u8)
{
    let mut data_file = File::open("data.txt").unwrap();

    // Create an empty mutable string
    let mut file_content = String::new();

    // Copy contents of file to a mutable string
    data_file.read_to_string(&mut file_content).unwrap();
    let mut tmp:Vec<String>=vec![];
    for word in file_content.lines()
    {
        tmp.push(word.to_string());
    }
    for i in (0..tmp.len()).rev()
    {

        if tmp[i].parse::<u8>().unwrap()==key
        {
            return (tmp[i].parse::<u8>().unwrap(),tmp[i+1].parse::<u8>().unwrap());
        }
    }
    panic!("pair not found");
}

pub fn delete(&mut self,key:u8,value:u8)->()
{
   let mut data_file = OpenOptions::new()
        .append(true)
        .open(self.file)
        .expect("cannot open file");

    // Write to a file
    data_file
        .write(key.to_string().as_bytes())
        .expect("write failed");

    data_file
        .write(value.to_string().as_bytes())
        .expect("write failed");

    data_file
        .write("1".as_bytes())
        .expect("write failed");
}
  }
