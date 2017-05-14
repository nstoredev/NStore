using NStore.Aggregates;

namespace NStore.Sample.Domain.Room
{
    public class RoomMadeAvailable
    {

    }

    public class Room : Aggregate<RoomState>
    {
        public void MakeAvailable()
        {
            Raise(new RoomMadeAvailable());
        }
    }
}