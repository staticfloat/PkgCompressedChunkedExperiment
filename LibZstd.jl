module LibZstd

# This file contains various pieces of Zstd functionality.
# In particular, it allows us to parse the highest-level structure
# of a Zstd file (a ZstdFrame), which is very useful to us as we make
# great use of a particular property of Zstd, which is that a decoder
# must continue to decode multiple frames, concatenating their output
# as if they were a single frame.  This allows us to create "chunks" of
# compressed data that can be independently manipulated while still
# decompressing together to make a single, coherent output.  We will
# combine this with the content-defined chunking provided by tools like
# `casync`/`desync` to allow for rapid assembly of compressed on-disk
# tarballs from individual chunks.
#
# Future work may add a ZstdSeekable table to the end of the chunked
# zstd-compressed tarball to allow for fast random access to files
# within the tarball.

abstract type AbstractZstdHeader; end

export ZstdFrame
struct ZstdFrame <: AbstractZstdHeader
    # Position within the larger file of this frame header
    offset::UInt64

    # Size of the frame (header + payload)
    compressed_len::UInt64
    uncompressed_len::UInt64

    # Dictionary used to compress this frame (zero means no dictionary)
    dictionary_id::UInt32
end

struct ZstdSkippableFrame <: AbstractZstdHeader
    magic::UInt32
    offset::UInt64
    data::Vector{UInt8}
end

export list_frames
function list_frames(io::IO)
    frames = AbstractZstdHeader[]
    offset = UInt64(0)
    while !eof(io)
        magic = read(io, UInt32)
        magic_len = 4
        if magic >> 4 == 0x0184D2A5
            # This is a skippable frame, just skip it for now
            frame_size = read(io, UInt32)
            data = read(io, frame_size)
            push!(frames, ZstdSkippableFrame(magic, offset, data))
            offset += magic_len + 4 + length(data)
        elseif magic == 0xFD2FB528
            # Figure out the length of this frame
            frame_header_byte = read(io, UInt8)
            FCS_Flag_Value = (frame_header_byte >> 6) & 0x3
            Single_Segment_flag = (frame_header_byte >> 5) & 0x1
            Window_Descriptor_Size = Single_Segment_flag == 0 ? 1 : 0
            Dictionary_ID_flag = (frame_header_byte & 0x3)
            Content_Checksum_flag = (frame_header_byte >> 2) & 0x1
            
            # See https://github.com/facebook/zstd/blob/v1.5.7/doc/zstd_compression_format.md#frame_header
            if FCS_Flag_Value == 0
                if Single_Segment_flag == 0
                    FCS_Field_Size = 0
                else
                    FCS_Field_Size = 1
                end
            elseif FCS_Flag_Value == 1
                FCS_Field_Size = 2
            elseif FCS_Flag_Value == 2
                FCS_Field_Size = 4
            else
                FCS_Field_Size = 8
            end

            if Dictionary_ID_flag == 0
                DID_Field_Size = 0
            elseif Dictionary_ID_flag == 1
                DID_Field_Size = 1
            elseif Dictionary_ID_flag == 2
                DID_Field_Size = 2
            else
                DID_Field_Size = 4
            end

            if Single_Segment_flag == 0
                # If the single segment flag is not set, skip the window descriptor
                skip(io, Window_Descriptor_Size)
            end

            # Skip the dictionary ID, if it exists
            if DID_Field_Size == 0
                dictionary_id = UInt32(0)
            elseif DID_Field_Size == 1
                dictionary_id = UInt32(read(io, UInt8))
            elseif DID_Field_Size == 2
                dictionary_id = UInt32(read(io, UInt16))
            elseif DID_Field_Size == 4
                dictionary_id = UInt32(read(io, UInt32))
            end

            if FCS_Field_Size == 0
                uncompressed_len = 0
            elseif FCS_Field_Size == 1
                uncompressed_len = UInt64(read(io, Uint8))
            elseif FCS_Field_Size == 2
                uncompressed_len = UInt64(read(io, UInt16)) + 256
            elseif FCS_Field_Size == 4
                uncompressed_len = UInt64(read(io, UInt32))
            else
                uncompressed_len = read(io, UInt64)
            end

            # See https://github.com/facebook/zstd/blob/v1.5.7/doc/zstd_compression_format.md#frame_header
            frame_header_len = 1 + Window_Descriptor_Size + DID_Field_Size + FCS_Field_Size

            # Next, skip through blocks until we finish this frame
            compressed_payload_len = 0
            num_blocks = 0
            while !eof(io)
                # Read block header
                block_header = UInt32(read(io, UInt16)) | (UInt32(read(io, UInt8)) << 16)
                last_block = block_header & 0x000001
                block_type = (block_header & 0x000006) >> 1

                # If this is an RLE block, our block size is always 1
                if block_type == 1
                    block_size = 1
                else
                    block_size = block_header >> 3
                end

                skip(io, block_size)
                compressed_payload_len += block_size
                num_blocks += 1
                if last_block != 0
                    break
                end
                if eof(io)
                    error("Ran into EOF before last block!")
                end
            end

            # Skip the ending content checksum, if it exists.
            checksum_len = 0
            if Content_Checksum_flag != 0
                checksum_len = 4
                skip(io, 4)
            end

            push!(frames, ZstdFrame(
                offset,
                magic_len + frame_header_len + num_blocks*3 + compressed_payload_len + checksum_len,
                uncompressed_len,
                dictionary_id,
            ))
            offset += magic_len + frame_header_len + num_blocks*3 + compressed_payload_len  + checksum_len
        else
            @warn("Got bad magic, not a zstd frame!", magic, offset)
            break
        end        
    end
    return frames
end

list_frames(path::String) = open(list_frames, path; read=true)

end # LibZstd
