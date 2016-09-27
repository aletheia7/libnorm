
#include "normApi.h"

#include "protoCheck.h"
#include "protoSocket.h"

#include <stdio.h>       // for printf(), etc
#include <stdlib.h>      // for srand()
#include <string.h>      // for strrchr(), memset(), etc
#include <sys/time.h>    // for gettimeofday()
#include <arpa/inet.h>   // for htons()
#include <fcntl.h>       // for, well, fnctl()
#include <errno.h>       // obvious child
#include <assert.h>      // embarrassingly obvious
#include <sys/mman.h>    // Memory Lock.

#define USE_FWRITE 0
const unsigned int LOOP_MAX = 100;

class NormStreamer
{
    public:
        NormStreamer();
        ~NormStreamer();
        
        // some day build these directly into NORM API
        enum CCMode {NORM_FIXED, NORM_CC, NORM_CCE, NORM_CCL};
        
        enum 
        {
            MSG_HEADER_SIZE = 2,    // Big Endian message length header size
            MSG_SIZE_MAX = 65535    // (including length header)  
        };  
            
        void SetLoopback(bool state)
        {
            loopback = state;
            if (NORM_SESSION_INVALID != norm_session)
                NormSetMulticastLoopback(norm_session, state);
        }       
        
        bool EnableUdpRelay(const char* relayAddr, unsigned short relayPort);
       	bool EnableUdpListener(unsigned short listenPort, const char* groupAddr, const char * interfaceName);
        bool UdpListenerEnabled() const
            {return input_socket.IsOpen();}
        bool UdpRelayEnabled() const
            {return output_socket.IsOpen();}
        
        int GetInputDescriptor() const
            {return (input_socket.IsOpen() ? input_socket.GetHandle() : fileno(input_file));}
        int GetOutputDescriptor() const
            {return (output_socket.IsOpen() ? output_socket.GetHandle() : fileno(output_file));} 
            
        bool OpenNormSession(NormInstanceHandle instance, 
                             const char*        addr,
                             unsigned short     port,
                             NormNodeId         nodeId);
        void CloseNormSession();
        
        void SetNormCongestionControl(CCMode ccMode);
        void SetNormTxRate(double bitsPerSecond)
        {
            assert(NORM_SESSION_INVALID != norm_session);
            NormSetTxRate(norm_session, bitsPerSecond);
        }
        void SetNormMulticastInterface(const char* ifaceName)
        {
            assert(NORM_SESSION_INVALID != norm_session);
            NormSetMulticastInterface(norm_session, ifaceName);
        } 
        void SetNormMessageTrace(bool state)
        {
            assert(NORM_SESSION_INVALID != norm_session);
            NormSetMessageTrace(norm_session, state);
        } 
        void AddAckingNode(NormNodeId ackId)
        {
            assert(NORM_SESSION_INVALID != norm_session);
            NormAddAckingNode(norm_session, ackId);
            norm_acking = true;  // invoke ack-based flow control
        }
        
        bool Start(bool sender, bool receiver);
        void Stop()
            {is_running = false;}
        bool IsRunning() const
            {return is_running;}
        void HandleNormEvent(const NormEvent& event);
        
        // Sender methods
        int GetInputFile() const
            {return input_fd;}
        void SetInputReady() 
            {input_ready = true;}
        bool InputReady() const
            {return input_ready;}
        bool InputNeeded() const
            {return input_needed;}
        void ReadInput();
        void ReadInputSocket();
        bool TxPending() const
            {return (!input_needed && (input_index < input_msg_length));}
        bool TxReady() const
            {return (tx_ready && (!norm_acking || (tx_stream_buffer_count < tx_stream_buffer_max)));}
        void SendData();
        unsigned int WriteToStream(const char* buffer, unsigned int numBytes);
        void FlushStream(bool eom, NormFlushMode flushMode);
        
        // Receiver methods
        bool RxNeeded() const
            {return rx_needed;}
        bool RxReady() const
            {return rx_ready;}
        void RecvData();
        int GetOutputFile() const
            {return output_fd;}
        void SetOutputReady()
            {output_ready = true;}
        bool OutputReady() const
            {return output_ready;}
        bool OutputPending() const
            {return (!rx_needed && (output_index < output_msg_length));}
        void WriteOutput();
        void WriteOutputSocket();
        
        void OmitHeader(bool state) 
            {omit_header = state;}
        
        // These can only be called post-OpenNormSession()
        void SetSilentReceiver(bool state)
            {NormSetSilentReceiver(norm_session, true);}
        void SetTxLoss(double txloss)
            {NormSetTxLoss(norm_session, txloss);}
            
    private:
        NormSessionHandle   norm_session;
        bool                is_multicast;
        bool                loopback;
        bool                is_running;     
                                                     
        // State variables for reading input messages for transmission
        ProtoSocket         input_socket;    // optional UDP socket to "listen"
        FILE*               input_file;
        int                 input_fd;      // stdin by default  
        bool                input_ready;                       
        bool                input_needed;    
        char                input_buffer[MSG_SIZE_MAX]; 
        unsigned int        input_msg_length;
        unsigned int        input_index;
                                                
        NormObjectHandle    tx_stream;
        bool                tx_ready;
        UINT16              tx_segment_size;
        unsigned int        tx_stream_buffer_max;
        unsigned int        tx_stream_buffer_threshold; // flow control threshold
        unsigned int        tx_stream_buffer_count;
        unsigned int        tx_stream_bytes_remain;
        bool                tx_watermark_pending;
        bool                norm_acking;
        NormFlushMode       flush_mode;  // TBD - allow for "none", "passive", "active" options
        
        // Receive stream and state variables for writing received messages to output
        NormObjectHandle    rx_stream;
        bool                rx_ready;
        bool                rx_needed;
        bool                msg_sync;
        ProtoSocket         output_socket;  // optional UDP socket for recv msg output
        ProtoAddress        relay_addr;     // dest addr for recv msg relay
        FILE*               output_file;
        int                 output_fd;    // stdout by default
        bool                output_ready;
        char                output_buffer[MSG_SIZE_MAX];
        unsigned int        output_msg_length;
        unsigned int        output_index;
        
        // These are some options mainly for testing purposes
        bool                omit_header;  // if "true", receive message length header is _not_ written to output
        //bool                rx_silent;
        //double              tx_loss;
            
};  // end class NormStreamer

NormStreamer::NormStreamer()
 : norm_session(NORM_SESSION_INVALID), is_multicast(false), loopback(false), is_running(false),
   input_socket(ProtoSocket::UDP), input_file(stdin), input_fd(fileno(stdin)), input_ready(true), 
   input_needed(false), input_msg_length(0), input_index(0),
   tx_stream (NORM_OBJECT_INVALID), tx_ready(true),  tx_segment_size(0), 
   tx_stream_buffer_max(0), tx_stream_buffer_count(0), tx_stream_bytes_remain(0), 
   tx_watermark_pending(false), norm_acking(false), flush_mode(NORM_FLUSH_ACTIVE),
   rx_stream(NORM_OBJECT_INVALID), rx_ready(false), rx_needed(false), msg_sync(false),
   output_socket(ProtoSocket::UDP), output_file(stdout), output_fd(fileno(stdout)), output_ready(true), 
   output_msg_length(0), output_index(0), 
   omit_header(false)//, rx_silent(false), tx_loss(0.0)
{
}


NormStreamer::~NormStreamer()
{
}

bool NormStreamer::EnableUdpRelay(const char* relayAddr, unsigned short relayPort)
{
    if (!output_socket.Open())
    {
        fprintf(stderr, "normStreamer output_socket open() error: %s\n", GetErrorString());
        return false;
    }
    if (!relay_addr.ResolveFromString(relayAddr))
    {
        fprintf(stderr, "normStreamer error: invalid relay address\n");
        return false;
    }
    relay_addr.SetPort(relayPort);  // TBD - validate port number??
    return true;
}  // end bool EnableUdpRelay()

bool NormStreamer::EnableUdpListener(unsigned short listenPort, const char* groupAddr, const char * interfaceName)
{
	bool rval = true ;
    if (!input_socket.Open(listenPort))
    {
        fprintf(stderr, "normStreamer input_socket open() error: %s\n", GetErrorString());
		rval = false ;
    }
	else
	{
    if (NULL != groupAddr)
    {
        ProtoAddress addr;
        if (!addr.ResolveFromString(groupAddr) || (!addr.IsMulticast()))
        {
            fprintf(stderr, "normStreamer error: invalid 'listen' group address\n");
				rval = false ;
        }
			else
			{
				if (!input_socket.JoinGroup(addr, interfaceName))
                {
                    fprintf(stderr, "normStreamer input_socket JoinGroup() error: %s\n", GetErrorString());
					        rval = false ;
                }
				else
				{
					fprintf(stderr, "%s:=>normStreamer joined group %s/%d on interface %s\n", __PRETTY_FUNCTION__,  groupAddr, listenPort, interfaceName) ;
					uint32_t rxSockBufferSize = 64*1024*1024 ;
					if ( input_socket.SetRxBufferSize(rxSockBufferSize))
					{
						fprintf(stderr, "normStreamer failed to set input socket buffer size (%d)\n", rxSockBufferSize);
						rval = false ;
                    }
				}
			}
		}
	}
	if ( rval )
	    fprintf(stderr, "%s:=>listening on port %d on socket %d\n", __PRETTY_FUNCTION__, listenPort, (int)input_socket.GetHandle());
	else
		fprintf(stderr, "%s:=>failed. listening on port %d on socket %d\n", __PRETTY_FUNCTION__, listenPort, (int)input_socket.GetHandle());
	return rval;
}  // end NormStreamer::EnableUdpListener()

bool NormStreamer::OpenNormSession(NormInstanceHandle instance, const char* addr, unsigned short port, NormNodeId nodeId)
{
    if (NormIsUnicastAddress(addr))
        is_multicast = false;
    else
        is_multicast = true;
    norm_session = NormCreateSession(instance, addr, port, nodeId);
    if (NORM_SESSION_INVALID == norm_session)
    {
        fprintf(stderr, "normStreamer error: unable to create NORM session\n");
        return false;
    }
    if (is_multicast)
    {
        NormSetRxPortReuse(norm_session, true);
        if (loopback)
            NormSetMulticastLoopback(norm_session, true);
    }
    
    // Set some default parameters (maybe we should put parameter setting in Start())
    NormSetDefaultSyncPolicy(norm_session, NORM_SYNC_STREAM);
    
    NormSetDefaultUnicastNack(norm_session, true);
    
    NormSetTxRobustFactor(norm_session, 20);
    
    return true;
}  // end NormStreamer::OpenNormSession()

void NormStreamer::CloseNormSession()
{
    if (NORM_SESSION_INVALID == norm_session) return;
    NormDestroySession(norm_session);
    norm_session = NORM_SESSION_INVALID;
}  // end NormStreamer::CloseNormSession()

void NormStreamer::SetNormCongestionControl(CCMode ccMode)
{
    assert(NORM_SESSION_INVALID != norm_session);
    switch (ccMode)
    {
        case NORM_CC:  // default TCP-friendly congestion control
            NormSetEcnSupport(norm_session, false, false, false);
            break;
        case NORM_CCE: // "wireless-ready" ECN-only congestion control
            NormSetEcnSupport(norm_session, true, true);
            break;
        case NORM_CCL: // "loss tolerant", non-ECN congestion control
            NormSetEcnSupport(norm_session, false, false, true);
            break;
        case NORM_FIXED: // "fixed" constant data rate
            NormSetEcnSupport(norm_session, false, false, false);
            break;
    }
    if (NORM_FIXED != ccMode)
        NormSetCongestionControl(norm_session, true);
    else
        NormSetCongestionControl(norm_session, false);
}  // end NormStreamer::SetNormCongestionControl()

bool NormStreamer::Start(bool sender, bool receiver)
{
    // TBD - make the NORM parameters command-line accessible
    unsigned int bufferSize = 256*1024*1024;
    unsigned int segmentSize = 1400;
    unsigned int blockSize = 64;
    unsigned int numParity = 0;
        
    if (receiver)
    {
        NormPreallocateRemoteSender(norm_session, segmentSize, blockSize, numParity, bufferSize);
        if (!NormStartReceiver(norm_session, bufferSize))
        {
            fprintf(stderr, "normStreamer error: unable to start NORM receiver\n");
            return false;
        }
		if (0 != mlockall(MCL_CURRENT | MCL_FUTURE))
		    fprintf(stderr, "normStreamer error: failed to lock memory for receiver.\n");
        NormSetRxSocketBuffer(norm_session, 6*1024*1024);
        rx_needed = true;
        rx_ready = false;
    }
    if (sender)
    {
        NormSetGrttEstimate(norm_session, 0.001);
        NormSetBackoffFactor(norm_session, 0.0);
        if (norm_acking)
        {   
            // ack-based flow control enabled on command-line, 
            // so disable timer-based flow control
            NormSetFlowControl(norm_session, 0.0);
        }
        // Pick a random instance id for now
        struct timeval currentTime;
        gettimeofday(&currentTime, NULL);
        srand(currentTime.tv_usec);  // seed random number generator
        NormSessionId instanceId = (NormSessionId)rand();
        if (!NormStartSender(norm_session, instanceId, bufferSize, segmentSize, blockSize, numParity))
        {
            fprintf(stderr, "normStreamer error: unable to start NORM sender\n");
            if (receiver) NormStopReceiver(norm_session);
            return false;
        }
        NormSetGrttEstimate(norm_session, 0.001);
        
        //NormSetGrttMax(norm_session, 0.090);
        //NormSetAutoParity(norm_session, 2);
        NormSetTxSocketBuffer(norm_session, 4*1024*1024);
        if (NORM_OBJECT_INVALID == (tx_stream = NormStreamOpen(norm_session, bufferSize)))
        {
            fprintf(stderr, "normStreamer error: unable to open NORM tx stream\n");
            NormStopSender(norm_session);
            if (receiver) NormStopReceiver(norm_session);
            return false;
        }
		else
		{
			if (0 != mlockall(MCL_CURRENT|MCL_FUTURE))
                fprintf(stderr, "normStreamer warning: failed to lock memory for sender.\n");
		}
        tx_segment_size = segmentSize;
        tx_stream_buffer_max = NormGetStreamBufferSegmentCount(bufferSize, segmentSize, blockSize);
        tx_stream_buffer_max -= blockSize;  // a little safety margin (perhaps not necessary)
        tx_stream_buffer_threshold = tx_stream_buffer_max / 8;
        tx_stream_buffer_count = 0;
        tx_stream_bytes_remain = 0;
        tx_watermark_pending = false;
        tx_ready = true;
        input_index = input_msg_length = 0;
        input_needed = true;
        input_ready = true;
    }
    is_running = true;
    return true;
}  // end NormStreamer::Start();

void NormStreamer::ReadInputSocket()
{
    unsigned int loopCount = 0;
    NormSuspendInstance(NormGetInstance(norm_session));
    while (input_needed && input_ready && (loopCount < LOOP_MAX))
    {
        loopCount++;
        unsigned int numBytes = MSG_SIZE_MAX - MSG_HEADER_SIZE;
        ProtoAddress srcAddr;
        if (input_socket.RecvFrom(input_buffer+MSG_HEADER_SIZE, numBytes, srcAddr))
        {
            if (0 == numBytes)
            {
                input_ready = false;
                break;
            }
            unsigned short msgSize = numBytes + MSG_HEADER_SIZE;
            msgSize = htons(msgSize);
            memcpy(input_buffer, &msgSize, MSG_HEADER_SIZE);
            input_index = 0;
            input_msg_length = numBytes;
            input_needed = false;
            if (TxReady()) SendData();
        }
        else
        {
            // TBD - handle error?
            input_ready = false;
        }
    }
    NormResumeInstance(NormGetInstance(norm_session));
}  // end NormStreamer::ReadInputSocket()

void NormStreamer::ReadInput()
{
    if (UdpListenerEnabled()) return ReadInputSocket();
    // The loop count makes sure we don't spend too much time here
    // before going back to the main loop to handle NORM events, etc
    unsigned int loopCount = 0;
    NormSuspendInstance(NormGetInstance(norm_session));
    while (input_needed && input_ready && (loopCount < LOOP_MAX))
    {
        loopCount++;
        //if (100 == loopCount)
        //    fprintf(stderr, "normStreamer ReadInput() loop count max reached\n");
        unsigned int numBytes;
        if (input_index < MSG_HEADER_SIZE)
        {
            // Reading message length header for next message to send
            numBytes = MSG_HEADER_SIZE - input_index;
        }
        else
        {
            // Reading message body
            assert(input_index < input_msg_length);
            numBytes = input_msg_length - input_index;
        }
#if USE_FWRITE
        size_t result = fread(input_buffer + input_index, 1, numBytes, input_file);
        if (result > 0)
        {
            input_index += result;
            if (MSG_HEADER_SIZE == input_index)
            {
                // We have now read the message size header
                // TBD - support other message header formats?
                // (for now, assume 2-byte message length header)
                uint16_t msgSize ;
                memcpy(&msgSize, input_buffer, MSG_HEADER_SIZE);
                msgSize = ntohs(msgSize);
                input_msg_length = msgSize;
            }
            else if (input_index == input_msg_length)
            {
                // Message input complete
                input_index = 0;  // reset index for transmission phase
                input_needed = false;
                if (TxReady()) SendData();
            }   
            else
            {
                // Still need more input
                // (wait for next input notification to read more)
                fprintf(stderr, "wairing for input\n);
                input_ready = false;
            }
        }
        else
        {
            if (feof(input_file))
            {
                // end-of-file reached, TBD - trigger final flushing and wrap-up
                fprintf(stderr, "normStreamer: input end-of-file detected ...\n");
                if (norm_acking)
                    NormSetWatermark(norm_session, tx_stream, true);
                NormStreamClose(tx_stream, true);
                input_needed = false;
            }
            else if (ferror(input_file))
            {
                switch (errno)
                {
                    case EINTR:
                        continue;  // interrupted, try again
                    case EAGAIN:
                        // input starved, wait for next notification
                        input_ready = false;
                        break;
                    default:
                        // TBD - handle this better
                        perror("normStreamer error reading input");
                        break;
                }
            }
            break;
        }
#else
        ssize_t result = read(input_fd, input_buffer + input_index, numBytes);
        if (result > 0)
        {
            input_index += result;
            if (MSG_HEADER_SIZE == input_index)
            {
                // We have now read the message size header
                // TBD - support other message header formats?
                // (for now, assume 2-byte message length header)
                uint16_t msgSize ;
                memcpy(&msgSize, input_buffer, MSG_HEADER_SIZE);
                msgSize = ntohs(msgSize);
                input_msg_length = msgSize;
            }
            else if (input_index == input_msg_length)
            {
                // Message input complete
                input_index = 0;  // reset index for transmission phase
                input_needed = false;
                if (TxReady()) SendData();
            }   
            else
            {
                // Still need more input
                // (wait for next input notification to read more)
                input_ready = false;
            }
        }
        else if (0 == result)
        {
            // end-of-file reached, TBD - trigger final flushing and wrap-up
            fprintf(stderr, "normStreamer: input end-of-file detected ...\n");
            if (norm_acking)
                NormSetWatermark(norm_session, tx_stream, true);
            NormStreamClose(tx_stream, true);
            input_needed = false;
        }
        else
        {
            switch (errno)
            {
                case EINTR:
                    continue;  // interrupted, try again
                case EAGAIN:
                    // input starved, wait for next notification
                    input_ready = false;
                    break;
                default:
                    // TBD - handle this better
                    perror("normStreamer error reading input");
                    break;
            }
            break;
        }
#endif // if/else USE_FWRITE
    }  // end while (input_needed && input_ready)
    NormResumeInstance(NormGetInstance(norm_session));
}  // end NormStreamer::ReadInput()

void NormStreamer::SendData()
{
    while (TxReady() && !input_needed)
    {
        // Note WriteToStream() or FlushStream() will set "tx_ready" to 
        // false upon flow control thus negating TxReady() status
        assert(input_index < input_msg_length);
        input_index += WriteToStream(input_buffer + input_index, input_msg_length - input_index);
        if (input_index == input_msg_length)
        {
            // Complete message was sent, so flush
            // TBD - provide flush "none", "passive", and "active" options
            //FlushStream(true, flush_mode);
            NormStreamMarkEom(tx_stream);
            input_index = input_msg_length = 0;
            input_needed = true;
        }
        else
        {
            fprintf(stderr, "SendData() impeded by flow control\n");
        }
    }  // end while (TxReady() && !input_needed)
}  // end NormStreamer::SendData()

unsigned int NormStreamer::WriteToStream(const char* buffer, unsigned int numBytes)
{
    unsigned int bytesWritten;
    if (norm_acking)
    {
        // This method uses NormStreamWrite(), but limits writes by explicit ACK-based flow control status
        if (tx_stream_buffer_count < tx_stream_buffer_max)
        {
            // 1) How many buffer bytes are available?
            unsigned int bytesAvailable = tx_segment_size * (tx_stream_buffer_max - tx_stream_buffer_count);
            bytesAvailable -= tx_stream_bytes_remain;  // unflushed segment portiomn
            if (numBytes <= bytesAvailable) 
            {
                unsigned int totalBytes = numBytes + tx_stream_bytes_remain;
                unsigned int numSegments = totalBytes / tx_segment_size;
                tx_stream_bytes_remain = totalBytes % tx_segment_size;
                tx_stream_buffer_count += numSegments;
            }
            else
            {
                numBytes = bytesAvailable;
                tx_stream_buffer_count = tx_stream_buffer_max;
            }
            // 2) Write to the stream
            bytesWritten = NormStreamWrite(tx_stream, buffer, numBytes);
            //assert(bytesWritten == numBytes);  // this could fail if timer-based flow control is left enabled
            // 3) Check if we need to issue a watermark ACK request?
            if (!tx_watermark_pending && (tx_stream_buffer_count >= tx_stream_buffer_threshold))
            {
                // Initiate flow control ACK request
                //fprintf(stderr, "initiating flow control ACK REQUEST\n");
                NormSetWatermark(norm_session, tx_stream);
                tx_watermark_pending = true;
            }
        }
        else
        {
            fprintf(stderr, "normStreamer: sender flow control limited\n");
            return 0;
        }
    }
    else
    {
        bytesWritten = NormStreamWrite(tx_stream, buffer, numBytes);
    }
    if (bytesWritten != numBytes) //NormStreamWrite() was (at least partially) blocked
    {
        fprintf(stderr, "NormStreamWrite() blocked by flow control ...\n");
        tx_ready = false;
    }
    return bytesWritten;
}  // end NormStreamer::WriteToStream()

void NormStreamer::FlushStream(bool eom, NormFlushMode flushMode)
{ 
    if (norm_acking)
    {
        bool setWatermark = false;
        if (0 != tx_stream_bytes_remain)
        {
            // The flush will force the runt segment out, so we increment our buffer usage count
            // (and initiate flow control watermark ack request if buffer mid-point threshold exceeded
            tx_stream_buffer_count++;
            tx_stream_bytes_remain = 0;
            if (!tx_watermark_pending && (tx_stream_buffer_count >= tx_stream_buffer_threshold))
            {
                setWatermark = true;
                tx_watermark_pending = true;
            }
            
        }
        // The check for "tx_watermark_pending" here prevents a new watermark
        // ack request from being set until the pending flow control ack is 
        // received. This favors avoiding dead air time over saving "chattiness"
        if (setWatermark)
        {
            // Flush passive since watermark will invoke active request
            // (TBD - do non-acking nodes NACK to watermark when not ack target?)
            NormStreamFlush(tx_stream, eom, NORM_FLUSH_PASSIVE);
        }
        else if (tx_watermark_pending)
        {
            // Pre-existing pending flow control watermark ack request
            NormStreamFlush(tx_stream, eom, flushMode);
        }
        else
        {
            // Since we're acking, use active ack request in lieu of active flush
            NormStreamFlush(tx_stream, eom, NORM_FLUSH_PASSIVE);
            if (NORM_FLUSH_ACTIVE == flushMode)
                setWatermark = true;
        }
        if (setWatermark) NormSetWatermark(norm_session, tx_stream, true);
    }
    else
    {
        NormStreamFlush(tx_stream, eom, flushMode);
    }
}  // end NormStreamer::FlushStream()

void NormStreamer::RecvData()
{    
    // The loop count makes sure we don't spend too much time here
    // before going back to the main loop to handle NORM events, etc
    unsigned int loopCount = 0;
    // Reads data from rx_stream to available output_buffer
    NormSuspendInstance(NormGetInstance(norm_session));
    while (rx_needed && rx_ready && (loopCount < LOOP_MAX))
    {
        loopCount++;
        //if (100 == loopCount)
        //    fprintf(stderr, "normStreamer RecvData() loop count max reached.\n");
        // Make sure we have msg_sync (TBD - skip this for byte streaming)
        if (!msg_sync)
        {
            msg_sync = NormStreamSeekMsgStart(rx_stream);
            if (!msg_sync) 
            {
                rx_ready = false;
                break;  // wait for next NORM_RX_OBJECT_UPDATED to re-sync
            }
        }
        unsigned int bytesWanted;
        if (output_index < MSG_HEADER_SIZE)
        {
            // Receiving message header
            bytesWanted = MSG_HEADER_SIZE - output_index;
        }
        else
        {
            // Receiving message body
            assert(output_index < output_msg_length);
            bytesWanted = output_msg_length - output_index;
        }
        unsigned bytesRead = bytesWanted;
        if (!NormStreamRead(rx_stream, output_buffer + output_index, &bytesRead))
        {
            // Stream broken (should _not_ happen if norm_acking flow control)
            //fprintf(stderr, "normStreamer error: broken stream detected, re-syncing ...\n");
            msg_sync = false;
            output_index = output_msg_length = 0;
            continue;
        }
        output_index += bytesRead;
        if (0 == bytesRead)
        {
            rx_ready = false;
        } 
        else if (bytesRead != bytesWanted)
        {
            continue;
            //rx_ready = false;  // didn't get all we need
        }
        else if (MSG_HEADER_SIZE == output_index)
        {
            // We have now read the message size header
            // TBD - support other message header formats?
            // (for now, assume 2-byte message length header)
            uint16_t msgSize ;
            memcpy(&msgSize, output_buffer, MSG_HEADER_SIZE);
            output_msg_length = ntohs(msgSize);
        }
        else if (output_index == output_msg_length)
        {
            // Received full message
            rx_needed = false;
            output_index = 0;  // reset for writing to output
            if (output_ready) WriteOutput();
        }
    }
    NormResumeInstance(NormGetInstance(norm_session));
    
}  // end NormStreamer::RecvData()

void NormStreamer::WriteOutputSocket()
{
    if (output_ready && !rx_needed)
    {
        assert(output_index < output_msg_length);
        unsigned int payloadSize = output_msg_length - MSG_HEADER_SIZE;
        unsigned int numBytes = payloadSize;
        if (output_socket.SendTo(output_buffer+MSG_HEADER_SIZE, numBytes, relay_addr))
        {
            if (numBytes != payloadSize)
            {
                // sendto() was blocked
                output_ready = false;
                return;
            }
            rx_needed = true;
            output_index = output_msg_length = 0;
        }
        else
        {
            output_ready = false;
        }
    }
}  // end NormStreamer::WriteOutputSocket()

void NormStreamer::WriteOutput()
{
    if (UdpRelayEnabled()) return WriteOutputSocket();
    while (output_ready && !rx_needed)
    {
        assert(output_index < output_msg_length);
#if USE_FWRITE
        size_t result = fwrite(output_buffer + output_index, 1, output_msg_length - output_index, output_file);
        if (result > 0)
        {
            output_index += result;
            if (output_index == output_msg_length)
            {
                // Complete message written
                fflush(output_file);
                rx_needed = true;
                output_index = output_msg_length = 0;
            }
            else
            {
                output_ready = false;
            }
        }
        else
        {
            if (feof(output_file))
            {
                // end-of-file reached, TBD - stop acting as receiver, signal sender we're done?
                fprintf(stderr, "normStreamer: output end-of-file detected ...\n");
                output_ready = false;
            }
            else if (ferror(output_file))
            {
                fprintf(stderr, "normStreamer: output error detected ...\n");
                switch (errno)
                {
                    case EINTR:
                        perror("normStreamer output EINTR");
                        continue;  // interupted, try again
                    case EAGAIN:
                        // output blocked, wait for next notification
                        perror("normStreamer output blocked");
                        output_ready = false;
                        break;
                    default:
                        perror("normStreamer error writing output");
                        break;
                }
            }
            return;
        }
#else
        ssize_t result = write(output_fd, output_buffer + output_index, output_msg_length - output_index);
        if (result >= 0)
        {
            output_index += result;
            if (output_index == output_msg_length)
            {
                // Complete message written
                rx_needed = true;
                output_index = output_msg_length = 0;
            }
            else
            {
                output_ready = false;
            }
        }
        else
        {
            switch (errno)
            {
                case EINTR:
                    perror("normStreamer output EINTR");
                    continue;  // interupted, try again
                case EAGAIN:
                    // output blocked, wait for next notification
                    //perror("normStreamer output blocked");
                    output_ready = false;
                    break;
                default:
                    perror("normStreamer error writing output");
                    break;
            }
            break;
        }
#endif  // if/else USE_FWRITE
    }
}  // end NormStreamer::WriteOutput()

void NormStreamer::HandleNormEvent(const NormEvent& event)
{
    bool logAllocs = false;
    switch (event.type)
    {
        case NORM_TX_QUEUE_EMPTY:
        case NORM_TX_QUEUE_VACANCY:
            tx_ready = true;
            break;
            
        case NORM_GRTT_UPDATED:
            //fprintf(stderr, "new GRTT = %lf\n", NormGetGrttEstimate(norm_session));
            break;
            
        case NORM_TX_WATERMARK_COMPLETED:
            if (NORM_ACK_SUCCESS == NormGetAckingStatus(norm_session))
            {
                //fprintf(stderr, "WATERMARK COMPLETED\n");
                if (tx_watermark_pending)
                {
                    // Flow control ack request was pending.
                    tx_watermark_pending = false;
                    tx_stream_buffer_count -= tx_stream_buffer_threshold;
                    //fprintf(stderr, "flow control ACK completed\n");
                }
            }
            else
            {
                // TBD - we could see who didn't ACK and possibly remove them
                //       from our acking list.  For now, we are infinitely
                //       persistent by resetting watermark ack request
                NormResetWatermark(norm_session);
            }
            break; 
            
        case NORM_TX_OBJECT_PURGED:
            // tx_stream graceful close completed
            NormStopSender(norm_session);
            tx_stream = NORM_OBJECT_INVALID;
            if (NORM_OBJECT_INVALID == rx_stream) Stop();
            break;
        
        case NORM_REMOTE_SENDER_INACTIVE:
            //fprintf(stderr, "REMOTE SENDER INACTIVE node: %u\n", NormNodeGetId(event.sender));
            //NormNodeDelete(event.sender);
            //logAllocs = true;
            break;
            
        case NORM_RX_OBJECT_NEW:
            if ((NORM_OBJECT_INVALID == rx_stream) &&
                (NORM_OBJECT_STREAM == NormObjectGetType(event.object)))
            {
                rx_stream = event.object;
                rx_ready = true;
                msg_sync = false;
                rx_needed = true;
                output_index = output_msg_length = 0;
            }
            else
            {
                fprintf(stderr, "normStreamer warning: NORM_RX_OBJECT_NEW while already receiving?!\n");
            }
            
        case NORM_RX_OBJECT_UPDATED:
            rx_ready = true;
            break;
        
        case NORM_RX_OBJECT_ABORTED:
            //fprintf(stderr, "NORM_RX_OBJECT_ABORTED\n");// %hu\n", NormObjectGetTransportId(event.object));
            //logAllocs = true;
            break;
            
        case NORM_RX_OBJECT_COMPLETED:
            // Rx stream has closed 
            // TBD - set state variables so any pending output is
            //       written out and things shutdown if not sender, too
            rx_ready = false;
            break;
            
        default:
            break;     
    }
    //NormReleasePreviousEvent(NormGetInstance(norm_session));

    if (logAllocs) 
    {
#ifdef USE_PROTO_CHECK
        ProtoCheckLogAllocations(stderr);
#endif // USE_PROTO_CHECK
    }
            
}  // end NormStreamer::HandleNormEvent()


void Usage()
{
    fprintf(stderr, "Usage: normStreamer id <nodeId> {send | recv} [addr <addr>[/<port>]][ack <node1>[,<node2>,...]\n"
                    "                    [cc|cce|ccl|rate <bitsPerSecond>][interface <name>][debug <level>][trace]\n"
                    "                    [listen [<mcastAddr>/]<port>][relay <dstAddr>/<port>]\n"
                    "                    [debug <level>][trace][log <logfile>]\n"
                    "                    [omit][silent][txloss <lossFraction>]\n");
}
int main(int argc, char* argv[])
{
    // REQUIRED parameters initiailization
    NormNodeId nodeId = NORM_NODE_NONE;
    bool send = false;
    bool recv = false;
    
    char sessionAddr[64];
    strcpy(sessionAddr, "224.1.2.3");
    unsigned int sessionPort = 6003;
    
    char listenAddr[64];  // optional mcast addr to listen for input
    listenAddr[0] = '\0';
    unsigned int listenPort = 0;  // optional UDP port to listen for input
    
    NormNodeId ackingNodeList[256]; 
    unsigned int ackingNodeCount = 0;
    
    double txRate = 0.0; // used for non-default NORM_FIXED ccMode
    NormStreamer::CCMode ccMode = NormStreamer::NORM_CC;
    const char* mcastIface = NULL;
    
    const char * listenerMcastIface = NULL ;
    bool loopback = false;
    int debugLevel = 0;
    bool trace = false;
    const char* logFile = NULL;
    bool omitHeaderOnOutput = false;
    bool silentReceiver = false;
    double txloss = 0.0;
    
    NormStreamer normStreamer;
    
    // Parse command-line
    int i = 1;
    while (i < argc)
    {
        const char* cmd = argv[i++];
        size_t len = strlen(cmd);
        if (0 == strncmp(cmd, "send", len))
        {
            send = true;
        }
        else if (0 == strncmp(cmd, "recv", len))
        {
            recv = true;
        }
        else if (0 == strncmp(cmd, "loopback", len))
        {
            loopback = true;
        }
        else if (0 == strncmp(cmd, "addr", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "normStreamer error: missing 'addr[/port]' value!\n");
                Usage();
                return -1;
            }
            const char* addrPtr = argv[i++];
            const char* portPtr = strchr(addrPtr, '/');
            if (NULL == portPtr)
            {
                strncpy(sessionAddr, addrPtr, 63);
                sessionAddr[63] = '\0';
            }
            else
            {
                size_t addrLen = portPtr - addrPtr;
                if (addrLen > 63) addrLen = 63;  // should issue error message
                strncpy(sessionAddr, addrPtr, addrLen);
                sessionAddr[addrLen] = '\0';
                portPtr++;
                sessionPort = atoi(portPtr);
            }
        }
        else if (0 == strncmp(cmd, "listen", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "normStreamer error: missing '[mcastAddr/]port]' value!\n");
                Usage();
                return -1;
            }
            const char* addrPtr = argv[i++];
            const char* portPtr = strchr(addrPtr, '/');
            if (NULL != portPtr)
            {
                size_t addrLen = portPtr - addrPtr;
                if (addrLen > 63) addrLen = 63;  // should issue error message
                strncpy(listenAddr, addrPtr, addrLen);
                listenAddr[addrLen] = '\0';
                portPtr++;
                addrPtr = listenAddr;
                listenPort = atoi(portPtr);
            }
            else
            {
                // no address, just port
                listenPort = atoi(addrPtr);
                addrPtr = NULL;
            }
            if (!normStreamer.EnableUdpListener(listenPort, addrPtr, listenerMcastIface))
            {
                fprintf(stderr, "normStreamer error: Failed to enable UDP listener\n") ;
                return -1;
            }
        }
        else if (0 == strncmp(cmd, "relay", len))
        {
            char relayAddr[64]; 
            relayAddr[0] = '\0';
            unsigned int relayPort = 0;
            if (i >= argc)
            {
                fprintf(stderr, "normStreamer error: missing relay 'dstAddr/port]' value!\n");
                Usage();
                return -1;
            }
            const char* addrPtr = argv[i++];
            const char* portPtr = strchr(addrPtr, '/');
            if (NULL == portPtr)
            {
                fprintf(stderr, "normStreamer error: missing relay 'port' value!\n");
                Usage();
                return -1;
            }
            if (NULL != portPtr)
            {
                size_t addrLen = portPtr - addrPtr;
                if (addrLen > 63) addrLen = 63;  // should issue error message
                strncpy(relayAddr, addrPtr, addrLen);
                relayAddr[addrLen] = '\0';
                portPtr++;
                relayPort = atoi(portPtr);
            }
            // TBD - check addr/port validity?
            normStreamer.EnableUdpRelay(relayAddr, relayPort);
        }
        else if (0 == strncmp(cmd, "id", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "normStreamer error: missing 'id' value!\n");
                Usage();
                return -1;
            }
            nodeId = atoi(argv[i++]);
        }
        else if (0 == strncmp(cmd, "ack", len))
        {
            // comma-delimited acking node id list
            if (i >= argc)
            {
                fprintf(stderr, "normStreamer error: missing 'id' <nodeId> value!\n");
                Usage();
                return -1;
            }
            const char* alist = argv[i++];
            while ((NULL != alist) && (*alist != '\0'))
            {
                // TBD - Do we need to skip leading white space?
                if (1 != sscanf(alist, "%d", ackingNodeList + ackingNodeCount))
                {
                    fprintf(stderr, "normStreamer error: invalid acking node list!\n");
                    Usage();
                    return -1;
                }
                ackingNodeCount++;
                alist = strchr(alist, ',');
                if (NULL != alist) alist++;  // point past comma
            }
        }
        else if (0 == strncmp(cmd, "rate", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "normStreamer error: missing 'rate' <bitsPerSecond> value!\n");
                Usage();
                return -1;
            }
            if (1 != sscanf(argv[i++], "%lf", &txRate))
            {
                fprintf(stderr, "normStreamer error: invalid transmit rate!\n");
                Usage();
                return -1;
            }       
            // set fixed-rate operation
            ccMode = NormStreamer::NORM_FIXED;     
        }
        else if (0 == strcmp(cmd, "cc"))
        {
            ccMode = NormStreamer::NORM_CC;
        }
        else if (0 == strcmp(cmd, "cce"))
        {
            ccMode = NormStreamer::NORM_CCE;
        }
        else if (0 == strcmp(cmd, "ccl"))
        {
            ccMode = NormStreamer::NORM_CCL;
        }
        else if (0 == strncmp(cmd, "interface", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "normStreamer error: missing 'interface' <name>!\n");
                Usage();
                return -1;
            }
            mcastIface = argv[i++];
        }
		else if (0 == strncmp(cmd, "linterface", len))
		{
			if (i >= argc)
			{
				fprintf(stderr, "normStreamer error: missing 'linterface' <name>!\n");
				Usage();
				return -1;
			}
			listenerMcastIface = argv[i++];
		}
        else if (0 == strncmp(cmd, "omit", len))
        {
            omitHeaderOnOutput = true;
        }
        else if (0 == strncmp(cmd, "silent", len))
        {
            silentReceiver = true;
        }
        else if (0 == strncmp(cmd, "txloss", len))
        {
            if (1 != sscanf(argv[i++], "%lf", &txloss))
            {
                fprintf(stderr, "normStreamer error: invalid 'txloss' value!\n");
                Usage();
                return -1;
            }
        }
        else if (0 == strncmp(cmd, "debug", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "normStreamer error: missing 'debug' <level>!\n");
                Usage();
                return -1;
            }
            debugLevel = atoi(argv[i++]);
        }
        else if (0 == strncmp(cmd, "trace", len))
        {
            trace = true;
        }
        else if (0 == strncmp(cmd, "log", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "normStreamer error: missing 'log' <fileName>!\n");
                Usage();
                return -1;
            }
            logFile = argv[i++];
        }
        else if (0 == strncmp(cmd, "help", len))
        {
            Usage();
            return 0;
        }
        else
        {
            fprintf(stderr, "normStreamer error: invalid command \"%s\"!\n", cmd);
            Usage();
            return -1;
        }
    }
    
    if (!send && !recv)
    {
        fprintf(stderr, "normStreamer error: not configured to send or recv!\n");
        Usage();
        return -1;
    }
    if (NORM_NODE_NONE == nodeId)
    {
        fprintf(stderr, "normStreamer error: no local 'id' provided!\n");
        Usage();
        return -1;
    }
    
    // TBD - should provide more error checking of calls
    NormInstanceHandle normInstance = NormCreateInstance();
    NormSetDebugLevel(debugLevel);
    if ((NULL != logFile) && !NormOpenDebugLog(normInstance, logFile))
    {
        perror("normStreamer error: unable to open log file");
        Usage();
        return -1;
    }
        
    
    normStreamer.SetLoopback(loopback);
    
    if (omitHeaderOnOutput) normStreamer.OmitHeader(true);
    
    if (!normStreamer.OpenNormSession(normInstance, sessionAddr, sessionPort, (NormNodeId)nodeId))
    {
        fprintf(stderr, "normStreamer error: unable to open NORM session\n");
        NormDestroyInstance(normInstance);
        return false;
    }
    
    if (silentReceiver) normStreamer.SetSilentReceiver(true);
    if (txloss > 0.0) normStreamer.SetTxLoss(txloss);
    
    for (unsigned int i = 0; i < ackingNodeCount; i++)
        normStreamer.AddAckingNode(ackingNodeList[i]);
    
    normStreamer.SetNormCongestionControl(ccMode);
    if (NormStreamer::NORM_FIXED == ccMode)
        normStreamer.SetNormTxRate(txRate);
    if (NULL != mcastIface)
        normStreamer.SetNormMulticastInterface(mcastIface);
    
    if (trace) normStreamer.SetNormMessageTrace(true);
    
    // TBD - set NORM session parameters
    normStreamer.Start(send, recv); 
    
    int normfd = NormGetDescriptor(normInstance);
    // Get input/output descriptors and set to non-blocking i/o
    int inputfd = normStreamer.GetInputDescriptor();
    int outputfd = normStreamer.GetOutputDescriptor();
    //if (!normStreamer.UdpListenerEnabled())
    {
        if (-1 == fcntl(inputfd, F_SETFL, fcntl(inputfd, F_GETFL, 0) | O_NONBLOCK))
            perror("normStreamer: fcntl(inputfd, O_NONBLOCK) error");
    }
#if !USE_FWRITE  
    // Don't set O_NONBLOCK when using fwrite()
    if (!normStreamer.UdpRelayEnabled())
    {
        if (-1 == fcntl(outputfd, F_SETFL, fcntl(outputfd, F_GETFL, 0) | O_NONBLOCK))
            perror("normStreamer: fcntl(outputfd, O_NONBLOCK) error");
    }
#endif
    fd_set fdsetInput, fdsetOutput;
    FD_ZERO(&fdsetInput);
    FD_ZERO(&fdsetOutput);
    while (normStreamer.IsRunning())
    {
        int maxfd = -1;
        // Only wait on NORM if needed for tx or rx readiness
        bool waitOnNorm = true;
        if (!(normStreamer.RxNeeded() || normStreamer.TxPending()))
            waitOnNorm = false; // no need to wait
        else if (normStreamer.RxNeeded() && normStreamer.RxReady())  
            waitOnNorm = false; // no need to wait
        else if (normStreamer.TxPending() && normStreamer.TxReady())
            waitOnNorm = false; // no need to wait
        if (waitOnNorm)
        {
            /*fprintf(stderr, "waiting on NORM event> txPending:%d txReady:%d rxNeeded:%d rxReady:%d\n", 
                             normStreamer.TxPending(), normStreamer.TxReady(),
                             normStreamer.RxNeeded(), normStreamer.RxReady());*/
            // we need to wait until NORM is tx_ready or rx_ready
            FD_SET(normfd, &fdsetInput);
            maxfd = normfd;
        }
        if (normStreamer.InputNeeded() && !normStreamer.InputReady())
        {   
            FD_SET(inputfd, &fdsetInput);
            if (inputfd > maxfd) maxfd = inputfd;
        }   
        else
        {
            FD_CLR(inputfd, &fdsetInput);
        }
        if (normStreamer.OutputPending() && !normStreamer.OutputReady())
        {
            FD_SET(outputfd, &fdsetOutput);
            if (outputfd > maxfd) maxfd = outputfd;
        }
        else
        {   
            FD_CLR(outputfd, &fdsetOutput);
        }
        if (maxfd >= 0)
        {
            int result = select(maxfd+1, &fdsetInput, &fdsetOutput, NULL, NULL);
            switch (result)
            {
                case -1:
                    switch (errno)
                    {
                        case EINTR:
                        case EAGAIN:
                            continue;
                        default:
                            perror("normStreamer select() error");
                            // TBD - stop NormStreamer
                            break;
                    }
                    break;
                case 0:
                    // shouldn't occur for now (no timeout)
                    continue;
                default:
                    if (FD_ISSET(inputfd, &fdsetInput))
                        normStreamer.SetInputReady();
                    if (FD_ISSET(outputfd, &fdsetOutput))
                        normStreamer.SetOutputReady();
                    break; 
            }
        }
        
        // We always clear out/handle pending NORM API events
        // (to keep event queue from building up)
        NormEvent event;
        while (NormGetNextEvent(normInstance, &event, false))
            normStreamer.HandleNormEvent(event);
        // As a result of input/output ready or NORM notification events:
        // 1) Recv from rx_stream if needed and ready
        if (normStreamer.RxNeeded() && normStreamer.RxReady())
            normStreamer.RecvData(); 
        // 2) Write any pending data to output if output is ready
        if (normStreamer.OutputPending() && normStreamer.OutputReady())
            normStreamer.WriteOutput();  
        // 3) Read from input if needed and ready 
        if (normStreamer.InputNeeded() && normStreamer.InputReady())
            normStreamer.ReadInput(); 
        // 4) Send any pending tx message
        if (normStreamer.TxPending() && normStreamer.TxReady())
            normStreamer.SendData();
        
    }  // end while(normStreamer.IsRunning()
    
    fprintf(stderr, "normStreamer exiting ...\n");
    
    fflush(stderr);
    
    NormDestroyInstance(normInstance);
    
}  // end main()

