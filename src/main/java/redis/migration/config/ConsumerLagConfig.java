package redis.migration.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by s.aravind on 15/02/18.
 */
@Getter
public class ConsumerLagConfig {

    private final String bootstrapServers;
    private final List<LagConfig> lagConfigs;

    @JsonCreator
    public ConsumerLagConfig(
            @NonNull @JsonProperty("boot_strap_servers") String bootstrapServers,
            @NonNull @JsonProperty("lagConfigs") List<LagConfig> lagConfigs) {
        this.bootstrapServers = bootstrapServers;
        this.lagConfigs = lagConfigs;
    }


    public static void main(String [] args) throws Exception {
        List<LagConfig> configs = new ArrayList<>();
        configs.add(new LagConfig("listing-nrt-ab", "AbUpdateTopology##listing-nrt-ab", 1000000L));
        configs.add(new LagConfig("santa_active_offers", "ActiveOffersTopology_rewrite_cluster##santa_active_offers", 1000000L));
        configs.add(new LagConfig("zulu-product-updates", "DiscoverStoreIngestionTopology_newkafka", 1000000L));
        configs.add(new LagConfig("lqs-updates", "LQSTopology_rewrite##lqs-updates", 1000000L));
        configs.add(new LagConfig("listing-pltag", "PlTagTopology_rewrite##listing-pltag", 1000000L));
        configs.add(new LagConfig("price-update-event", "PricingTopology_clustered##price-update-event", 1000000L));
        configs.add(new LagConfig("price-update-event-express", "PricingTopology_clustered_xpress##price-update-event-express", 1000000L));
        configs.add(new LagConfig("promise-lzn-updates", "PromiseUpdates_lzn_rewrite##promise-lzn-updates", 1000000L));
        configs.add(new LagConfig("promise-availability-updates", "PromiseUpdates_rewrite##promise-availability-updates", 1000000L));
        configs.add(new LagConfig("promise-shipping-type-updates", "PromiseUpdates_rewrite##promise-shipping-type-updates", 1000000L));
        configs.add(new LagConfig("promise-sla-updates", "PromiseUpdates_rewrite##promise-sla-updates", 1000000L));
        configs.add(new LagConfig("santa_olm", "SantaListingTagsIngestionTopology_rewrite##santa_olm", 1000000L));
        configs.add(new LagConfig("zulu-listing-p1", "ZuluListingTopology_P1##zulu-listing-p1", 1000000L));
        configs.add(new LagConfig("zulu-listing-p2", "ZuluListingTopology_P2##zulu-listing-p2", 1000000L));
        configs.add(new LagConfig("zulu-listing-p3", "ZuluListingTopology_P3##zulu-listing-p3", 1000000L));
        configs.add(new LagConfig("zulu-product-updates", "ZuluProductIngestionTopology_base##zulu-product-updates", 1000000L));
        configs.add(new LagConfig("zulu-listing-updates", "ZuluMetaIngestionTopology_base##zulu-listing-updates", 1000000L));
        configs.add(new LagConfig("listing_meta_replay", "ZuluMetaIngestionTopology_basereplay##listing_meta_replay", 1000000L));
        configs.add(new LagConfig("zpe_delta_index_replay", "ZuluProductIngestionTopology_basereplay##zpe_delta_index_replay", 1000000L));
        configs.add(new LagConfig("zulu-product-tags", "ZuluProductIngestionTopology_basetags##zulu-product-tags", 1000000L));


        ConsumerLagConfig consumerLagConfig = new ConsumerLagConfig("10.32.221.229:9092,10.32.29.243:9092,10.32.165.234:9092,10.32.1.21:9092,10.32.13.39:9092,10.32.117.251:9092,10.34.89.4:9092,10.34.45.188:9092,10.34.69.230:9092,10.34.49.38:9092,10.32.69.27:9092,10.34.109.138:9092,10.34.189.111:9092,10.34.33.188:9092,10.34.1.32:9092,10.34.161.125:9092,10.32.237.170:9092,10.34.137.200:9092,10.34.93.252:9092,10.34.181.211:9092", configs);
        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writeValueAsString(consumerLagConfig));

    }

}
