import { EntitySchema } from 'typeorm';

export const TradesEntity = new EntitySchema({
  name: 'trades',
  tableName: 'trades',
  columns: {
    trade_id: { type: 'bigint', primary: true, generated: true },
    pool_id: { type: 'bigint' },
    pair_contract: { type: 'text' },
    action: { type: 'text' },
    direction: { type: 'text' },
    offer_asset_denom: { type: 'text', nullable: true },
    offer_amount_base: { type: 'numeric', precision: 78, scale: 0, nullable: true },
    ask_asset_denom: { type: 'text', nullable: true },
    ask_amount_base: { type: 'numeric', precision: 78, scale: 0, nullable: true },
    return_amount_base: { type: 'numeric', precision: 78, scale: 0, nullable: true },
    is_router: { type: 'boolean', default: false },
    reserve_asset1_denom: { type: 'text', nullable: true },
    reserve_asset1_amount_base: { type: 'numeric', precision: 78, scale: 0, nullable: true },
    reserve_asset2_denom: { type: 'text', nullable: true },
    reserve_asset2_amount_base: { type: 'numeric', precision: 78, scale: 0, nullable: true },
    height: { type: 'bigint', nullable: true },
    tx_hash: { type: 'text', nullable: true },
    signer: { type: 'text', nullable: true },
    msg_index: { type: 'int', nullable: true },
    created_at: { type: 'timestamptz', primary: true },
  },
});
