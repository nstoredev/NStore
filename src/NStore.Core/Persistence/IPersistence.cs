﻿using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public interface IPersistence : IPartitionPersistence, IGlobalPersistence
    {
    }
}