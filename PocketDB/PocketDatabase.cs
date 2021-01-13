using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using BplusDotNet;
using System.IO;

namespace ShahrukhYousafzai.PocketDB
{
    public class PocketDatabase
    {
        private BplusTreeBytes tree = null;
        private string filename;
        private string path;
        private bool open = false;

        JsonSerializerSettings defaultSettings = new JsonSerializerSettings
        {
            PreserveReferencesHandling = PreserveReferencesHandling.All,
        };
        /// <summary>
        /// Checks if the pocket file is open.
        /// </summary>
        public bool IsOpen()
        {
            return open;
        }



        /// <summary>
        /// Gets the name of the current pocket file. Returns null if the pocket file wasn't initialized.
        /// </summary>
        public string GetName()
        {
            return filename;
        }

        /// <summary>
        /// Gets the path where the current pocket file is being saved. Returns null if the pocket file wasn't initialized.
        /// </summary>
        public string GetPath()
        {
            return path;
        }

        /// <summary>
        /// Initializes a new instance of the class. Pocket files will be saved on the Environment.GetFolderPath(Environment.SpecialFolder.Personal) with the specified filename.
        /// </summary>
        /// <param name="filename">Name of the pocket file without file extension</param>
        public PocketDatabase(String filename) : this(filename, Environment.GetFolderPath(Environment.SpecialFolder.Personal) + "/" + Path.GetFileName(Path.GetDirectoryName(System.AppDomain.CurrentDomain.BaseDirectory))) { }

        /// <summary>
        /// Initializes a new instance of the class. Pocket files will be saved on the specified path with the specified filename.
        /// </summary>
        /// <param name="filename">Name of the pocket file without file extension.</param>
        /// <param name="path">Path to the folder that will contain the pocket files</param>
        public PocketDatabase(String filename, String path)
        {
            string ExecFolderName = Path.GetFileName(Path.GetDirectoryName(System.AppDomain.CurrentDomain.BaseDirectory));

            Directory.CreateDirectory(Environment.GetFolderPath(Environment.SpecialFolder.Personal) + "/" + ExecFolderName);

            this.OpenPocketFile(filename, path);
        }

        /// <summary>
        /// Opens again a closed PocketDB
        /// </summary>
        public void ReOpen()
        {
            this.OpenPocketFile(this.GetName(), this.GetPath());
        }

        private void OpenPocketFile(String filename, String path)
        {
            if (this.IsOpen())
            {
                Console.WriteLine("You are trying to open an already opened pocket file (" + "\"" + filename + "\")");
                return;
            }
            try
            {
                tree = BplusTreeBytes.Initialize(System.IO.Path.Combine(path, filename + ".pocket"), System.IO.Path.Combine(path, filename + ".block"), 255);
            }
            catch (System.IO.IOException)
            {
                try
                {
                    tree = BplusTreeBytes.ReOpen(System.IO.Path.Combine(path, filename + ".pocket"), System.IO.Path.Combine(path, filename + ".block"));
                }
                catch (System.IO.DirectoryNotFoundException e)
                {
                    Console.WriteLine("Error while opening the pocket file, check that the specified directory exists\n" + e);
                    return;
                }
                catch (System.IO.IOException e)
                {
                    Console.WriteLine("Error while opening the pocket file, check that pocket file" + " \"" + filename + "\" " + "is not already open\n" + e);
                    return;
                }
            }
            this.open = true;
            this.filename = filename;
            this.path = path;
        }

        /// <summary>
        /// Inserts a new key-value pair into the database and commits the changes.
        /// </summary>
        /// <param name="key">Key that identifies the value</param>
        /// <param name="value">Object to be stored</param>
        public void Set(string key, object value)
        {
            if (!this.IsOpen())
            {
                Console.WriteLine("You are trying to use \"set\" on a pocket file that is not open. (" + "\"" + filename + "\")");
                return;
            }
            string json = JsonConvert.SerializeObject(value, defaultSettings);
            byte[] bytes = Encoding.ASCII.GetBytes(json);
            tree[key] = bytes;
            tree.Commit();
        }

        /// <summary>
        /// Gets an object stored in the database identified by a key.
        /// </summary>
        /// <param name="key">Key that identified the object</param>
        /// <param name="defaultValue">(optional) Value to use if the key was not found</param>
        /// <returns>T</returns>
        public T Get<T>(string key, T defaultValue = default(T))
        {
            if (!this.IsOpen())
            {
                Console.WriteLine("You are trying to use \"get\" on a pocket file that is not open. (" + "\"" + filename + "\")");
                return default(T);
            }
            if (tree.ContainsKey(key))
            {
                byte[] bytes = tree[key];
                string json = Encoding.ASCII.GetString(bytes);
                return JsonConvert.DeserializeObject<T>(json, defaultSettings);
            }
            return defaultValue;
        }

        /// <summary>
        /// Deletes a key and its value from the database and commits the changes. Does nothing if the key doesn't exist.
        /// </summary>
        /// <param name="key">Key to delete</param>
        public void Delete(string key)
        {
            if (!this.IsOpen())
            {
                Console.WriteLine("You are trying to use \"delete\" on a pocket file that is not open. (" + "\"" + filename + "\")");
                return;
            }
            try
            {
                tree.RemoveKey(key);
                tree.Commit();
            }
            catch (BplusDotNet.BplusTreeKeyMissing)
            {
                //ignore when trying to delete an unexisting key
            }
        }

        /// <summary>
        /// Checks if a key exists in the database.
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns>True if it exists, false otherwise.</returns>
        public bool HasKey(string key)
        {
            if (!this.IsOpen())
            {
                Console.WriteLine("You are trying to use \"hasKey\" on a pocket file that is not open. (" + "\"" + filename + "\")");
                return false;
            }
            return tree.ContainsKey(key);
        }

        /// <summary>
        /// Gets a list of all the keys in the database. This operation may be slow.
        /// </summary>
        /// <returns>A list of keys in the database.</returns>
        public List<string> GetKeys()
        {
            if (!this.IsOpen())
            {
                Console.WriteLine("You are trying to use \"getKeys\" on a pocket file that is not open. (" + "\"" + filename + "\")");
                return null;
            }
            List<string> keys = new List<string>();
            string key = tree.FirstKey();
            while (key != null)
            {
                keys.Add(key);
                key = tree.NextKey(key);
            }
            return keys;
        }

        /// <summary>
        /// Shuts down the database, closing the file streams.
        /// </summary>
        public void Close()
        {
            if (!this.IsOpen())
            {
                Console.WriteLine("You are trying to close a pocket file that is not open. (" + "\"" + filename + "\")");
                return;
            }
            tree.Abort();
            tree.Shutdown();
            this.open = false;
        }

        /// <summary>
        /// Tries to delete a pocket file in the Environment.GetFolderPath(Environment.SpecialFolder.Personal) with the specified filename.
        /// </summary>
        /// <param name="filename">Name of the pocket file without file extension.</param>
        public static void DeletePocketFile(string filename)
        {
            string ExecFolderName = Path.GetFileName(Path.GetDirectoryName(System.AppDomain.CurrentDomain.BaseDirectory));

            DeletePocketFile(filename, Environment.GetFolderPath(Environment.SpecialFolder.Personal) + "/" + ExecFolderName);
        }

        /// <summary>
        /// Tries to delete a pocket file in the specified path
        /// </summary>
        /// <param name="filename">Name of the pocket file without file extension.</param>
        /// <param name="path">Path where the pocket file is located</param>
        public static void DeletePocketFile(string filename, string path)
        {
            try
            {
                File.Delete(System.IO.Path.Combine(path, filename + ".pocket"));
                File.Delete(System.IO.Path.Combine(path, filename + ".block"));
            }
            catch (System.IO.IOException e)
            {
                Console.WriteLine("Error while deleting the pocket file, check that pocket file" + " \"" + filename + "\" " + "is not open and that it exists.\n" + e);
            }
        }

        /// <summary>
        /// Tries to get a list of pocket files in the Environment.GetFolderPath(Environment.SpecialFolder.Personal)
        /// </summary>
        /// <returns>A list containing the pocket file names</returns>
        public static String[] GetPocketFileList()
        {
            string ExecFolderName = Path.GetFileName(Path.GetDirectoryName(System.AppDomain.CurrentDomain.BaseDirectory));

            return GetPocketFileList(Environment.GetFolderPath(Environment.SpecialFolder.Personal) + "/" + ExecFolderName);
        }

        /// <summary>
        /// Tries to get a list of pocket files in the specified path
        /// </summary>
        /// <param name="path">Path of the folder</param>
        /// <returns>A list containing the pocket file names</returns>
        public static String[] GetPocketFileList(string path)
        {
            DirectoryInfo info = new DirectoryInfo(path);
            FileInfo[] fileInfo = info.GetFiles("*.pocket");
            String[] result = new String[fileInfo.Length];
            for (int i = 0; i < fileInfo.Length; ++i)
            {
                result[i] = Path.GetFileNameWithoutExtension(fileInfo[i].Name);
            }
            return result;
        }
    }
}
