using System;
namespace Models
{
    public class SecurityModel
    {
        public SecurityModel(Int32 id, string name)
        {
            Id = id;
            Name = name;
        }

        public Int32 Id { get; set; }

        public string Name { get; set; }
    }
}
