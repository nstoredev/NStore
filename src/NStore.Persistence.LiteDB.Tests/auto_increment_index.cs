using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using NStore.Persistence.Tests;
using Xunit;

namespace NStore.Persistence.LiteDB.Tests
{
    public class when_two_stores_try_to_append_with_automatic_index : BasePersistenceTest
    {
        public when_two_stores_try_to_append_with_automatic_index() : base(false)
        {
        }

        [Fact]
        public async Task index_should_be_increased()
        {
            var persistence1 = Create(true);
            var first = await persistence1.AppendAsync("stream", "a");
            Clear(persistence1, false);

            var persistence2 = Create(false);
            var second = await persistence2.AppendAsync("stream", "b");
            Clear(persistence2, true);

            Assert.Equal(1, first.Index);
            Assert.Equal(2, second.Index);
        }
    }
}
