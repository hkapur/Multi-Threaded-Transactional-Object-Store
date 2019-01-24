extern char *xacto_packet_type_names[];

void proto_debug_packet(XACTO_PACKET *pkt, char *payload);
void proto_init_packet(XACTO_PACKET *pkt, XACTO_PACKET_TYPE type, size_t size);
