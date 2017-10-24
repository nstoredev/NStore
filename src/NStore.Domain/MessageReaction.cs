using System;

namespace NStore.Domain
{
    public sealed class MessageReaction
    {
        public Object MessageIn { get; private set; }
        public Object[] MessagesOut { get; private set; }

        public MessageReaction(object messageIn, object[] messagesOut)
        {
            MessageIn = messageIn;
            MessagesOut = messagesOut;
        }
    }
}