using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Domain
{
	public interface IRepository
	{
		Task<T> GetByIdAsync<T>(
			string id
		) where T : IAggregate;

		Task<T> GetByIdAsync<T>(
			string id,
			CancellationToken cancellationToken
		) where T : IAggregate;

		Task SaveAsync<T>(
			T aggregate,
			string operationId
		) where T : IAggregate;

		Task SaveAsync<T>(
			T aggregate,
			string operationId,
			Action<IHeadersAccessor> headers
		) where T : IAggregate;

		Task SaveAsync<T>(
			T aggregate,
			string operationId,
			Action<IHeadersAccessor> headers,
			CancellationToken cancellationToken
		) where T : IAggregate;

		/// <summary>
		/// Clear all internal identity maps, you need to reload aggregates
		/// to be able to save again. <br />
		/// Useful if you got a concurrency exception and you need to reload
		/// the aggregate to retry.
		/// </summary>
		/// <returns></returns>
		void Clear();
	}
}