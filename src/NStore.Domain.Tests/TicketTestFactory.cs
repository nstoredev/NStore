namespace NStore.Domain.Tests
{
    public static class TicketTestFactory
    {
        public static Ticket ForTest(string id = "Ticket_1", bool init = true)
        {
            var ticket = new Ticket();

            if (init)
                ticket.Init(id);

            return ticket;
        }

        public static Ticket Sold(string id = "Ticket_1")
        {
            var ticket = ForTest(id);
            ticket.Sale();
            return ticket;
        }
    }
}