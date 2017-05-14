using NStore.Aggregates;

namespace NStore.Sample.Domain.Room
{
    public class RoomMadeAvailable
    {
        public string Id { get; private set; }

        public RoomMadeAvailable(string id)
        {
            this.Id = id;
        }
    }

    public class Room : Aggregate<RoomState>
    {
        public void MakeAvailable()
        {
            Raise(new RoomMadeAvailable(this.Id));
        }
    }
}