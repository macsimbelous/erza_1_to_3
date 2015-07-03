using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
namespace erza_1_to_3
{
    class Program
    {
        static public string Erza3_ConnectionString = @"data source=C:\temp\Erza3.sqlite";
        static void Main(string[] args)
        {
            List<CImage> img_list = new List<CImage>();
            #region ReadSqlite
            using (SQLiteConnection connection = new SQLiteConnection(@"data source=C:\utils\erza\erza.sqlite"))
            {
                connection.Open();
                using (SQLiteCommand command = new SQLiteCommand())
                {

                    command.CommandText = "select * from hash_tags";
                    command.Connection = connection;
                    SQLiteDataReader reader = command.ExecuteReader();
                    int count = 0;
                    while (reader.Read())
                    {
                        CImage img = new CImage();
                        img.hash = (byte[])reader["hash"];
                        img.hash_str = BitConverter.ToString(img.hash).Replace("-", string.Empty).ToLower();
                        img.is_deleted = (bool)reader["is_deleted"];
                        if (!System.Convert.IsDBNull(reader["tags"]))
                        {
                            img.tags_string = (string)reader["tags"];
                        }
                        if (!Convert.IsDBNull(reader["file_name"]))
                        {
                            img.file = (string)reader["file_name"];
                        }
                        img_list.Add(img);
                        count++;
                        Console.Write("\r" + count.ToString("#######"));
                    }
                    reader.Close();
                    Console.WriteLine("\rВсего: " + (count++).ToString());
                }
            }
            #endregion
            ExportTagsToMariaDB(img_list);
            ExportImagesToMariaDB(img_list);
            ExportImageTagsToMariaDB(img_list);
        }
        static void ExportTagsToMariaDB(List<CImage> img_list)
        {
            using (SQLiteConnection connection = new SQLiteConnection(Erza3_ConnectionString))
            {
                Console.WriteLine("Получаем уникальные теги");
                List<string> all_tags = new List<string>();
                foreach (CImage img in img_list)
                {
                    if (img.tags.Count > 0)
                    {
                        all_tags.AddRange(img.tags);
                    }
                }
                all_tags = all_tags.Distinct().ToList();
                all_tags.Sort();
                Console.WriteLine("\nТегов: " + all_tags.Count.ToString());
                Console.WriteLine("Загружаем теги в Базуданных");
                connection.Open();
                SQLiteTransaction transact = connection.BeginTransaction();
                for (int i = 0; i < all_tags.Count; i++)
                {
                    add_tag_to_db_not_verify(all_tags[i], connection);
                    Console.Write("\rДобавлено: {0}", i.ToString("000000"));
                }
                transact.Commit();
                Console.WriteLine();
            }
        }
        static void ExportImagesToMariaDB(List<CImage> img_list)
        {
            Console.WriteLine();
            using (SQLiteConnection connection = new SQLiteConnection(Erza3_ConnectionString))
            {
                connection.Open();
                SQLiteTransaction transact = connection.BeginTransaction();
                for (int i = 0; i < img_list.Count; i++)
                {
                    string ins = "INSERT INTO images (is_deleted, hash, file_path) VALUES (@is_deleted, @hash, @file_path);";
                    using (SQLiteCommand ins_command = new SQLiteCommand(ins, connection))
                    {
                        ins_command.Parameters.AddWithValue("hash", img_list[i].hash_str);
                        ins_command.Parameters.AddWithValue("is_deleted", img_list[i].is_deleted);
                        if (string.IsNullOrEmpty(img_list[i].file))
                        {
                            ins_command.Parameters.AddWithValue("file_path", System.DBNull.Value);
                        }
                        else
                        {
                            ins_command.Parameters.AddWithValue("file_path", img_list[i].file);
                        }
                        ins_command.ExecuteNonQuery();
                    }
                    Console.Write("\rДобавляем картинки: {0}", i.ToString("######"));
                }
                transact.Commit();
            }
        }
        static void ExportImageTagsToMariaDB(List<CImage> img_list)
        {
            Console.WriteLine("\nФормируем image_tags");
            List<image_tags> it = new List<image_tags>();
            int count = 0;
            using (SQLiteConnection connection = new SQLiteConnection(Erza3_ConnectionString))
            {
                connection.Open();
                SQLiteTransaction transact = connection.BeginTransaction();
                foreach (CImage img in img_list)
                {
                    if (img.tags.Count > 0)
                    {
                        List<long> tag_ids = GetTagIDs(img.tags, connection);
                        InsertImageTagsMass(GetImageID(img.hash_str, connection), tag_ids, connection);
                    }
                    count++;
                    Console.Write("{0}\\{1}\r", count, img_list.Count);
                }
                transact.Commit();
            }
            Console.WriteLine("Размер image_tags: {0}\n", it.Count);
        }
        static void InsertImageTagsMass(long image_id, List<long> tag_ids, SQLiteConnection connection)
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("INSERT INTO image_tags (image_id, tag_id) VALUES ");
            for (int i = 0; i < tag_ids.Count; i++)
            {
                if (i > 0) sql.Append(", ");
                sql.Append("(" + image_id.ToString() + ", " + tag_ids[i].ToString() + ")");
            }
            sql.Append(";");
            using (SQLiteCommand ins_command = new SQLiteCommand(sql.ToString(), connection))
            {
                ins_command.ExecuteNonQuery();
            }
        }
        static List<long> GetTagIDs(List<string> tags, SQLiteConnection connection)
        {
            List<long> ids = new List<long>();
            StringBuilder ins_quwery = new StringBuilder("SELECT tag_id FROM tags WHERE ");
            for (int i = 0; i < tags.Count; i++)
            {
                if (i == 0)
                {
                    ins_quwery.Append("tag = '");
                    ins_quwery.Append(tags[i].Replace("\'", "\'\'"));
                    ins_quwery.Append("'");
                }
                else
                {
                    ins_quwery.Append(" OR tag = '");
                    ins_quwery.Append(tags[i].Replace("\'", "\'\'"));
                    ins_quwery.Append("'");
                }
            }
            using (SQLiteCommand command = new SQLiteCommand(ins_quwery.ToString(), connection))
            {
                SQLiteDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    ids.Add((long)reader[0]);
                }
                reader.Close();
            }
            return ids;
        }
        static long GetImageID(string hash, SQLiteConnection connection)
        {
            using (SQLiteCommand command = new SQLiteCommand("SELECT image_id FROM images WHERE hash = @hash;", connection))
            {
                command.Parameters.AddWithValue("hash", hash);
                return (long)command.ExecuteScalar();
            }
        }
        public static void add_tag_to_db_not_verify(string tag, SQLiteConnection connection)
        {
            string ins = "INSERT INTO tags (tag) VALUES (@tag);";
            using (SQLiteCommand ins_command = new SQLiteCommand(ins, connection))
            {
                ins_command.Parameters.AddWithValue("tag", tag);
                ins_command.ExecuteNonQuery();
            }
        }
    }
    public class CImage
    {
        public long image_id;
        public long file_id;
        public bool is_new = true;
        public bool is_deleted = false;
        public long id;
        public byte[] hash;
        public string file = null;
        public List<string> tags = new List<string>();
        public string hash_str;
        public string tags_string
        {
            get
            {
                string s = String.Empty;
                for (int i = 0; i < tags.Count; i++)
                {
                    if (i > 0)
                    {
                        s = s + " ";
                    }
                    s = s + tags[i];
                }
                return s;
            }
            set
            {
                string[] t = value.Split(' ');
                for (int i = 0; i < t.Length; i++)
                {
                    if (t[i].Length > 0)
                    {
                        tags.Add(t[i]);
                    }
                }
            }
        }
        public override string ToString()
        {
            if (this.file != String.Empty)
            {
                return file.Substring(file.LastIndexOf('\\') + 1);
            }
            else
            {
                return "No File!";
            }
        }
    }
    public class image_tags
    {
        public int tag_id;
        public int image_id;
        public image_tags(int _tag_id, int _image_id)
        {
            this.tag_id = _tag_id;
            this.image_id = _image_id;
        }
    }
}
