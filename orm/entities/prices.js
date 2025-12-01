import { EntitySchema } from 'typeorm';

export const PricesEntity = new EntitySchema({
  name: 'prices',
  tableName: 'prices',
  columns: {
    price_id: { type: 'bigint', primary: true, generated: true },
    token_id: { type: 'bigint' },
    pool_id: { type: 'bigint' },
    price_in_zig: { type: 'numeric', precision: 38, scale: 18 },
    is_pair_native: { type: 'boolean' },
    updated_at: { type: 'timestamptz' },
  },
});
