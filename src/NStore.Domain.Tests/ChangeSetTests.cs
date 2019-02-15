using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace NStore.Domain.Tests
{
    public class ChangesetTests
    {
        [Fact]
        public void should_create_with_version_and_events()
        {
            var changeset = new Changeset
            (
                aggregateVersion: 1,
                events: new object[] { "a", "b" }
            );

            Assert.Equal(1, changeset.AggregateVersion);
            Assert.Collection(changeset.Events,
                evt => { Assert.Equal("a", evt); },
                evt => { Assert.Equal("b", evt); }
            );

            Assert.False(changeset.IsEmpty());

            Assert.False(changeset.Headers.Keys.Any());
        }

        [Fact]
        public void should_be_empty_when_has_no_events()
        {
            var changeset = new Changeset(1, new object[0]);
            Assert.True(changeset.IsEmpty());
        }

        [Fact]
        public void should_restore_events_and_headers()
        {
            var changeset = new Changeset
            (
                aggregateVersion: 1,
                events: new object[] { "a", "b" },
                headers: new Dictionary<string, object>()
                {
                    { "key", "value"}
                }
            );

            Assert.Equal(1, changeset.AggregateVersion);
            Assert.Collection(changeset.Events,
                evt => { Assert.Equal("a", evt); },
                evt => { Assert.Equal("b", evt); }
            );

            Assert.False(changeset.IsEmpty());
            Assert.Collection(changeset.Headers, kv =>
            {
                Assert.Equal("key", kv.Key);
                Assert.Equal("value", kv.Value);
            });
        }
    }
}
