using System;

namespace NStore.Core.Tests.Processing
{
    internal class TargetException : Exception
    {
    }

    internal class Target
    {
        public string Param { get; private set; }

        public void FailPublic(object p)
        {
            throw new TargetException();
        }

        public void FailPublicConditionally(object p)
        {
            if (p == null)
            {
                throw new TargetException();
            }
        }

        private void FailPrivate(object p)
        {
            throw new TargetException();
        }

        /// <summary>
        /// Simple method that accepts an object and return void.
        /// </summary>
        /// <param name="param"></param>
        public void DoSomething(string param)
        {
            Param = param;
        }

        /// <summary>
        /// Simple method that accepts an object and return void.
        /// </summary>
        /// <param name="param"></param>
        private void DoSomethingPrivate(string param)
        {
            Param = param;
        }

        /// <summary>
        /// Simple method that accepts an object and return void.
        /// </summary>
        /// <param name="param"></param>
        public string DoSomethingReturn(string param)
        {
            Param = param;
            return $"processed {param}";
        }

        /// <summary>
        /// Simple method that accepts an object and return void.
        /// </summary>
        /// <param name="param"></param>
        public string DoSomethingWithObjectReturn(object param)
        {
            return "processed";
        }

        public object CallDoSomethingReturn(string param)
        {
            return DoSomethingReturn(param);
        }
    }
}
